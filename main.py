from astrbot.api.event import filter, AstrMessageEvent, MessageEventResult, MessageChain
from astrbot.api.star import Context, Star, register
from astrbot.api import logger
from astrbot.api.message_components import Plain
from datetime import datetime, timedelta, timezone
import os
import re
import json
import asyncio
import subprocess
from typing import List, Dict, Optional, Set

# 北京时间时区 (UTC+8)
CHINA_TZ = timezone(timedelta(hours=8))

# 任务执行状态常量
TASK_PENDING = "pending"
TASK_RUNNING = "running"
TASK_FAILED = "failed"
TASK_SUCCEEDED = "succeeded"

def generate_task_id(task: Dict) -> str:
    """安全生成任务ID"""
    if "unified_msg_origin" not in task:
        return f"invalid_task_{datetime.now().timestamp()}"
    
    try:
        _, _, session_id = task["unified_msg_origin"].split(':', 2)
        script_name = re.sub(r'[^\w]', '_', task["script_name"])[:20]
        return f"{script_name}_{task['time'].replace(':', '')}_{session_id[:10]}"
    except Exception:
        return f"fallback_id_{datetime.now().timestamp()}"

def safe_datetime(dt: any) -> Optional[datetime]:
    """安全转换各种格式的时间数据为datetime对象"""
    if isinstance(dt, datetime):
        return dt.astimezone(CHINA_TZ)
    
    if not dt:
        return None
        
    try:
        # 尝试不同格式解析
        if isinstance(dt, str):
            # ISO格式解析
            return datetime.fromisoformat(dt).astimezone(CHINA_TZ)
        return dt
    except:
        return None

@register("ZaskManager", "xiaoxin", "全功能定时任务插件", "5.0.4", "https://github.com/styy88/ZaskManager")
class ZaskManager(Star):
    def __init__(self, context: Context):
        super().__init__(context)
        
        # 插件数据目录
        self.plugin_root = os.path.abspath(
            os.path.join(
                os.path.dirname(__file__),
                "..", "..",
                "plugin_data",
                "ZaskManager"
            )
        )
        os.makedirs(self.plugin_root, exist_ok=True)
        self.tasks_file = os.path.join(self.plugin_root, "tasks.json")
        self.executed_tracker_file = os.path.join(self.plugin_root, "executed.json")

        self.tasks: List[Dict] = []
        self._load_tasks()
        
        # 状态管理
        self.task_locks = {}  # 任务ID: 锁对象
        self.running_tasks = {}  # 任务ID: 任务开始时间
        
        # 加载持久化执行记录
        self.today_tasks_executed = self._load_executed_tasks()
        self.last_check_day = datetime.now(CHINA_TZ).date()  # 上次检查的日期
        
        # 启动定时器
        self.schedule_checker_task = asyncio.create_task(self._schedule_checker())
        self.task_monitor_task = asyncio.create_task(self._task_monitor())
        
        logger.info("ZaskManager 插件初始化完成，定时任务调度器已启动")

    def _load_executed_tasks(self) -> Set[str]:
        """加载已执行任务记录（持久化）"""
        try:
            if os.path.exists(self.executed_tracker_file) and os.path.getsize(self.executed_tracker_file) > 0:
                with open(self.executed_tracker_file, "r") as f:
                    executed_ids = json.load(f)
                    logger.info(f"加载 {len(executed_ids)} 个历史执行记录")
                    return set(executed_ids)
        except Exception as e:
            logger.error(f"执行记录加载失败: {str(e)}")
        
        # 创建空的执行记录文件
        with open(self.executed_tracker_file, "w") as f:
            json.dump([], f)
        return set()

    def _save_executed_tasks(self) -> None:
        """保存已执行任务记录（持久化）"""
        try:
            with open(self.executed_tracker_file, "w") as f:
                json.dump(list(self.today_tasks_executed), f)
            logger.debug("执行记录已持久化")
        except Exception as e:
            logger.error(f"执行记录保存失败: {str(e)}")

    def _load_tasks(self) -> None:
        """安全加载任务，兼容所有时间格式"""
        try:
            if os.path.exists(self.tasks_file) and os.path.getsize(self.tasks_file) > 0:
                try:
                    with open(self.tasks_file, "r", encoding="utf-8") as f:
                        raw_tasks = json.load(f)
                    
                    # 迁移和修复任务数据
                    self.tasks = []
                    for task in raw_tasks:
                        if ("unified_msg_origin" not in task or 
                            "script_name" not in task or 
                            "time" not in task):
                            continue
                            
                        # 创建规范化任务对象
                        new_task = {
                            "task_id": task.get("task_id", generate_task_id(task)),
                            "script_name": task["script_name"],
                            "time": task["time"],
                            "unified_msg_origin": task["unified_msg_origin"],
                            "status": task.get("status", TASK_PENDING),
                            "retry_count": task.get("retry_count", 0),
                            "created": safe_datetime(task.get("created")),
                            "last_attempt": safe_datetime(task.get("last_attempt")),
                            "last_run": safe_datetime(task.get("last_run")),
                        }
                            
                        self.tasks.append(new_task)
                    
                    logger.info(f"成功加载 {len(self.tasks)} 个有效任务")
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    logger.error(f"任务文件解析错误: {str(e)}")
                    # 创建损坏文件的备份
                    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
                    backup_file = f"{self.tasks_file}.corrupted.{timestamp}.bak"
                    os.rename(self.tasks_file, backup_file)
                    logger.warning(f"已将损坏的任务文件备份为: {backup_file}")
                    self.tasks = []
                    logger.info("已重置任务列表（文件损坏）")
            else:
                self.tasks = []
                if not os.path.exists(self.tasks_file):
                    logger.info("任务文件不存在，已初始化空任务列表")
                else:
                    logger.info("任务文件为空，已初始化空任务列表")
                
            # 立即保存规范化后的数据
            self._save_tasks()
        except Exception as e:
            logger.error(f"任务加载失败: {str(e)}")
            self.tasks = []
            # 尝试创建空白任务文件
            try:
                with open(self.tasks_file, "w", encoding="utf-8") as f:
                    json.dump([], f, indent=2, ensure_ascii=False)
            except:
                pass

    def _save_tasks(self) -> None:
        """安全保存任务，避免保存无效对象"""
        try:
            # 准备可以JSON序列化的任务数据
            save_tasks = []
            for task in self.tasks:
                # 创建任务副本
                save_task = task.copy()
                
                # 转换时间字段为字符串
                for time_field in ["last_run", "last_attempt", "created"]:
                    dt = save_task.get(time_field)
                    if isinstance(dt, datetime):
                        save_task[time_field] = dt.isoformat()
                    elif dt and not isinstance(dt, str):
                        # 移除非时间非字符串字段
                        save_task[time_field] = None
                
                # 确保没有其他非JSON可序列化对象
                save_tasks.append({k: v for k, v in save_task.items() if v is None or isinstance(v, (str, int, float, bool, list, dict))})
            
            # 保存到文件
            with open(self.tasks_file, "w", encoding="utf-8") as f:
                json.dump(save_tasks, f, indent=2, ensure_ascii=False)
            logger.debug("任务数据已持久化")
        except Exception as e:
            logger.error(f"任务保存失败: {str(e)}")

    async def _schedule_checker(self) -> None:
        """定时任务检查器（每秒检查一次）""" 
        logger.info("定时检查器启动（每秒检查一次）")
        while True:
            try:
                # 每秒检查一次
                await asyncio.sleep(1)
                
                now = datetime.now(CHINA_TZ)
                current_date = now.date()
                
                # 检查日期是否变更
                if self.last_check_day != current_date:
                    logger.info(f"日期变更：{self.last_check_day} -> {current_date}")
                    
                    # 日期变更 - 重置所有非运行状态的任务
                    reset_count = 0
                    for task in self.tasks:
                        if task["status"] != TASK_RUNNING:
                            task["status"] = TASK_PENDING
                            task["retry_count"] = 0
                            reset_count += 1
                    
                    # 清空当日执行记录
                    self.today_tasks_executed.clear()
                    self._save_executed_tasks()
                    self.last_check_day = current_date
                    
                    logger.info(f"日期变更，已重置 {reset_count} 个任务状态")
                
                # 当前时间格式化为 HH:MM
                current_hour_min = now.strftime("%H:%M")
                
                # 遍历所有任务
                for task in self.tasks:
                    task_id = task["task_id"]
                    
                    # 检查任务是否今日已执行过
                    if task_id in self.today_tasks_executed:
                        continue
                        
                    # 检查状态是否适合执行
                    if task["status"] not in [TASK_PENDING, TASK_FAILED]:
                        continue
                        
                    # 获取任务时间并规范化为 HH:MM 格式
                    task_time = task["time"]
                    
                    # 跳过无效时间格式
                    if not isinstance(task_time, str):
                        logger.warning(f"任务 {task_id} 时间格式无效")
                        continue
                    
                    # 尝试标准化时间格式
                    normalized_time = None
                    try:
                        # HH:MM 格式
                        if ':' in task_time and len(task_time) == 5:
                            parts = task_time.split(':')
                            hour = int(parts[0])
                            minute = int(parts[1])
                            if 0 <= hour < 24 and 0 <= minute < 60:
                                normalized_time = task_time
                        # HHMM 格式
                        elif len(task_time) == 4 and task_time.isdigit():
                            hour = int(task_time[:2])
                            minute = int(task_time[2:])
                            if 0 <= hour < 24 and 0 <= minute < 60:
                                normalized_time = f"{hour:02d}:{minute:02d}"
                    except:
                        pass
                    
                    if not normalized_time:
                        logger.warning(f"任务 {task_id} 时间格式无效: {task_time}")
                        continue
                    
                    # 比较当前时间与任务时间（允许1分钟的时间窗口）
                    if current_hour_min != normalized_time:
                        # 计算时间差
                        try:
                            now_minutes = int(current_hour_min[:2]) * 60 + int(current_hour_min[3:5])
                            target_minutes = int(normalized_time[:2]) * 60 + int(normalized_time[3:5])
                            time_diff = abs(now_minutes - target_minutes)
                            if time_diff > 1:  # 超过1分钟就不执行
                                continue
                        except Exception as e:
                            logger.error(f"计算时间差失败: {e}")
                            continue
                    
                    # 获取任务锁
                    if task_id not in self.task_locks:
                        self.task_locks[task_id] = asyncio.Lock()
                        
                    # 尝试执行任务
                    if not self.task_locks[task_id].locked():
                        logger.info(f"符合执行条件: {task_id} [{current_hour_min}]")
                        asyncio.create_task(self._execute_task_with_lock(task, now))
                    else:
                        logger.warning(f"任务 {task_id} 已在执行中，跳过重复触发")
                
            except asyncio.CancelledError:
                logger.info("定时检查器被取消")
                break
            except Exception as e:
                logger.error(f"定时检查器错误: {str(e)}")
                await asyncio.sleep(10)

    async def _task_monitor(self) -> None:
        """任务状态监控器（每6小时检查一次）"""
        logger.info("任务状态监控器启动")
        while True:
            try:
                await asyncio.sleep(21600)  # 6小时
                now = datetime.now(CHINA_TZ)
                today_start = datetime(now.year, now.month, now.day, tzinfo=CHINA_TZ)
                
                # 检查未执行的任务（没有今天执行记录）
                missed_tasks = []
                for task in self.tasks:
                    if task["status"] == TASK_SUCCEEDED:
                        continue
                        
                    last_run = safe_datetime(task.get("last_run"))
                    if not last_run or last_run < today_start:
                        missed_tasks.append(task)
                
                # 只在日志中记录错过的任务
                if missed_tasks:
                    logger.warning(f"发现 {len(missed_tasks)} 个未执行的任务")
                    for task in missed_tasks[:3]:  # 只记录前3个避免过多
                        task_id = task["task_id"]
                        last_run = safe_datetime(task.get("last_run"))
                        last_run_str = last_run.strftime("%Y-%m-%d %H:%M") if last_run else "从未执行"
                        logger.debug(f"未执行任务: ID={task_id}, 脚本={task['script_name']}, 最后执行={last_run_str}")
                
                # 每3天检查一次卡住的任务
                if now.weekday() % 3 == 0:  # 每3天一次
                    await self._check_stuck_tasks()
                    
            except asyncio.CancelledError:
                logger.info("任务监控器被取消")
                break
            except Exception as e:
                logger.error(f"任务监控器错误: {str(e)}")
                await asyncio.sleep(3600)

    async def _check_stuck_tasks(self) -> None:
        """检查并恢复卡住的任务（超过5分钟未完成）"""
        now = datetime.now(CHINA_TZ)
        stuck_tasks = []
        
        for task_id, start_time in self.running_tasks.items():
            if (now - start_time).total_seconds() > 300:  # 超过5分钟
                task = next((t for t in self.tasks if t["task_id"] == task_id), None)
                if task:
                    logger.warning(f"检测到卡住的任务: {task_id}")
                    stuck_tasks.append(task_id)
        
        # 移除并重试卡住的任务
        for task_id in stuck_tasks:
            self.running_tasks.pop(task_id, None)
            task = next((t for t in self.tasks if t["task_id"] == task_id), None)
            if task:
                task["status"] = TASK_FAILED
                task["retry_count"] += 1
                logger.warning(f"任务 {task_id} 被标记为失败 (卡住)")
        if stuck_tasks:
            self._save_tasks()

    async def _execute_task_with_lock(self, task: Dict, now: datetime) -> None:
        """带锁执行任务（防止并发）"""
        task_id = task["task_id"]
        task_lock = self.task_locks.get(task_id)
        if not task_lock:
            logger.warning(f"任务 {task_id} 的锁对象不存在")
            return
            
        async with task_lock:
            try:
                # 标记任务今日已执行
                self.today_tasks_executed.add(task_id)
                self._save_executed_tasks()  # ⚠️ 关键 - 立即持久化
                
                # 更新任务状态
                task["status"] = TASK_RUNNING
                task["last_attempt"] = now
                
                # 记录开始时间
                self.running_tasks[task_id] = now
                
                # 执行脚本
                script_name = task["script_name"]
                output = await self._execute_script(script_name)
                await self._send_task_result(task, output)
                
                # 更新状态
                task["last_run"] = now
                task["status"] = TASK_SUCCEEDED
                task["retry_count"] = 0
                
                logger.info(f"任务 {task_id} 执行成功")
                
            except Exception as e:
                # 执行失败后移除标记，允许重试
                if task_id in self.today_tasks_executed:
                    self.today_tasks_executed.remove(task_id)
                    self._save_executed_tasks()
                
                logger.error(f"任务 {task_id} 执行失败: {str(e)}")
                task["status"] = TASK_FAILED
                task["retry_count"] = min(task.get("retry_count", 0) + 1, 3)
                
                # 尝试发送错误信息
                try:
                    error_msg = f"⚠️ 脚本执行失败\n🔧 {task['script_name']}\n🕒 {now.strftime('%H:%M')}\n❌ {str(e)[:100]}"
                    await self.context.send_message(
                        task["unified_msg_origin"],
                        MessageChain([Plain(text=error_msg)])
                    )
                except Exception as send_err:
                    logger.error(f"错误通知发送失败: {send_err}")
                    
            finally:
                # 清理状态
                self.running_tasks.pop(task_id, None)
                self._save_tasks()
                self._save_executed_tasks()  # ⚠️ 关键 - 最终保存

    async def _execute_script(self, script_name: str) -> str:
        """执行指定脚本并返回输出"""
        # 安全处理脚本名称
        script_name = script_name.replace('.', '').replace('/', '').replace('\\', '')[:50]
        script_path = os.path.join(self.plugin_root, f"{script_name}.py")
        
        # 检查脚本是否存在
        if not os.path.exists(script_path):
            available_scripts = []
            try:
                for f in os.listdir(self.plugin_root):
                    if f.endswith('.py'):
                        available_scripts.append(f.replace('.py', ''))
            except Exception as e:
                logger.error(f"获取脚本列表失败: {str(e)}")
            available = ", ".join(available_scripts) if available_scripts else "无可用脚本"
            raise FileNotFoundError(f"脚本不存在！可用脚本: {available}")

        try:
            logger.info(f"执行脚本: {script_path}")
            proc = await asyncio.create_subprocess_exec(
                "python", script_path,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            
            # 等待执行完成
            try:
                stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=60)
            except asyncio.TimeoutError:
                if proc.returncode is None:
                    proc.kill()
                    await proc.communicate()
                raise TimeoutError("脚本执行超时 (60秒)")
            
            # 检查执行结果
            if proc.returncode != 0:
                error_msg = stderr.decode("utf-8", "ignore")[:300] if stderr else "无错误信息"
                raise RuntimeError(f"执行失败 (代码 {proc.returncode}): {error_msg}")
            
            # 返回输出
            output = stdout.decode("utf-8", "ignore") if stdout else ""
            return output
        except Exception as e:
            raise RuntimeError(f"执行错误: {str(e)}")

    async def _send_task_result(self, task: Dict, message: str) -> None:
        """发送任务结果"""
        if not message or not task or not task.get("unified_msg_origin"):
            return
            
        try:
            if not message.strip():
                message = "✅ 定时任务执行完毕（无输出）"
            
            # 文本消息（截断长文本）
            if len(message) > 1500:
                message = message[:1400] + "... [输出过长被截断]"
            
            message_chain = MessageChain([Plain(text=message)])
            await self.context.send_message(
                task["unified_msg_origin"],
                message_chain
            )
        except Exception as e:
            logger.error(f"消息发送失败: {str(e)}")

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("定时")
    async def schedule_command(self, event: AstrMessageEvent) -> MessageEventResult:
        """处理定时任务相关指令"""
        try:
            # 分割命令参数
            parts = event.message_str.strip().split(maxsplit=3)
            if len(parts) < 2:
                raise ValueError("请输入 `/定时 帮助` 查看使用说明")
                
            command = parts[1].lower()
            args = parts[2:] if len(parts) > 2 else []
            
            # 命令路由
            if command == "添加":
                if len(args) != 2:
                    raise ValueError("格式错误，应为: /定时 添加 [脚本名] [时间(如14:00)]")
                async for msg in self._add_task(event, args[0], args[1]):
                    yield msg
                    
            elif command == "删除":
                if not args:
                    raise ValueError("格式错误，应为: /定时 删除 [任务ID]")
                async for msg in self._delete_task(event, args[0]):
                    yield msg
                    
            elif command in ["列出", "列表"]:
                async for msg in self._list_tasks(event):
                    yield msg

            elif command == "状态":
                task_id = args[0] if args else None
                async for msg in self._task_status(event, task_id):
                    yield msg
                    
            else:
                async for msg in self._show_help(event):
                    yield msg
                    
        except Exception as e:
            yield event.plain_result(f"❌ 命令执行出错: {str(e)}")
            
    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("执行")
    async def execute_command(self, event: AstrMessageEvent) -> MessageEventResult:
        """立即执行脚本命令"""
        try:
            # 获取脚本名
            parts = event.message_str.strip().split(maxsplit=1)
            if len(parts) < 2:
                raise ValueError("格式错误，应为: /执行 [脚本名]")
                
            script_name = parts[1]
            
            # 执行脚本
            result = await self._execute_script(script_name)
            
            # 返回文本结果
            if len(result) > 1500:
                result = result[:1400] + "... [输出过长被截断]"
                
            yield event.plain_result(result)
            
        except Exception as e:
            yield event.plain_result(f"❌ 脚本执行失败: {str(e)}")
            
    async def _add_task(self, event: AstrMessageEvent, name: str, time_str: str) -> MessageEventResult:
        """添加定时任务（绑定当前会话的 unified_msg_origin）"""
        if not name or not time_str:
            raise ValueError("参数不能为空，格式：/定时 添加 [脚本名] [时间]")
        try:
            # 安全处理名称和时间
            name = name.strip()[:50]
            time_str = time_str.strip()
            
            # 验证时间格式（HH:MM）
            normalized_time = None
            try:
                # HH:MM 格式
                if ':' in time_str:
                    parts = time_str.split(':')
                    if len(parts) == 2:
                        hour = int(parts[0])
                        minute = int(parts[1])
                        if 0 <= hour < 24 and 0 <= minute < 60:
                            normalized_time = f"{hour:02d}:{minute:02d}"
                
                # HHMM 格式
                elif len(time_str) == 4 and time_str.isdigit():
                    hour = int(time_str[:2])
                    minute = int(time_str[2:])
                    if 0 <= hour < 24 and 0 <= minute < 60:
                        normalized_time = f"{hour:02d}:{minute:02d}"
            except:
                pass
            
            if not normalized_time:
                raise ValueError("时间格式应为 HH:MM（24小时制），例如：14:00")
            
            # 获取当前会话的唯一标识
            unified_msg_origin = event.unified_msg_origin

            # 检查脚本存在性
            script_path = os.path.join(self.plugin_root, f"{name}.py")  
            if not os.path.exists(script_path):
                available = ""
                try:
                    script_files = [f for f in os.listdir(self.plugin_root) if f.endswith('.py')]
                    available = ", ".join(f.replace('.py', '') for f in script_files[:5])  # 最多显示5个
                    if len(script_files) > 5:
                        available += f" 等{len(script_files)}个脚本"
                except:
                    available = "无法获取可用脚本列表"
                    
                raise FileNotFoundError(f"脚本不存在！可用脚本: {available or '无'}")

            # 构建新任务
            new_task = {
                "script_name": name,
                "time": normalized_time,
                "unified_msg_origin": unified_msg_origin,
                "last_run": None,
                "last_attempt": None,
                "status": TASK_PENDING,
                "retry_count": 0,
                "created": datetime.now(CHINA_TZ)
            }
            new_task["task_id"] = generate_task_id(new_task)  # 生成唯一ID
            
            # 检查同一会话、同脚本、同时间的任务冲突
            for existing_task in self.tasks:
                if (existing_task["unified_msg_origin"] == unified_msg_origin and 
                    existing_task["script_name"] == name and 
                    existing_task["time"] == normalized_time):
                    raise ValueError(f"同一会话下，{name} 脚本在 {normalized_time} 的任务已存在")
                
            self.tasks.append(new_task)
            self._save_tasks()
        
            # 构造回复
            reply_msg = (
                "✅ 定时任务创建成功\n"
                f"名称：{name}\n"
                f"时间：每日 {normalized_time}\n"
                f"任务ID：{new_task['task_id']}\n"
                f"状态：⏳ 待触发"
            )
            yield event.plain_result(reply_msg)
        except Exception as e:
            raise RuntimeError(f"添加任务失败: {str(e)}")

    async def _delete_task(self, event: AstrMessageEvent, identifier: str) -> MessageEventResult:
        """删除当前会话（unified_msg_origin 匹配）的任务"""
        try:
            current_unified = event.unified_msg_origin
            
            # 找到当前会话的所有任务
            current_tasks = [
                t for t in self.tasks 
                if t["unified_msg_origin"] == current_unified
            ]
            
            if not current_tasks:
                yield event.plain_result("当前会话没有定时任务")
                return
                
            deleted_tasks = []
            
            # 处理ID格式
            identifier = identifier.strip().lower()
            
            for task in current_tasks.copy():
                task_id = task["task_id"].lower()
                script_name = task["script_name"].lower()
                
                # 匹配任务ID或脚本名称
                if identifier == task_id or identifier == script_name:
                    # 如果任务正在运行，尝试取消
                    task_id_normal = task["task_id"]
                    if task_id_normal in self.running_tasks:
                        logger.warning(f"尝试取消正在运行的任务: {task_id_normal}")
                        self.running_tasks.pop(task_id_normal, None)
                        
                    self.tasks.remove(task)
                    deleted_tasks.append(task)
                    
            if not deleted_tasks:
                # 显示前5个任务的ID
                task_ids = "\n".join([f"· {t['task_id']} ({t['script_name']})" for t in current_tasks][:5])
                more = f"\n...等 {len(current_tasks)} 个任务" if len(current_tasks) > 5 else ""
                raise ValueError(f"未找到匹配任务，当前可用任务：\n{task_ids}{more}")
                
            self._save_tasks()
            
            # 构造删除报告
            report = ["✅ 已删除以下任务："]
            for task in deleted_tasks:
                report.append(
                    f"▫️ {task['script_name']} ({task['time']})\n"
                    f"  任务ID：{task['task_id']}\n"
                    "━━━━━━━━━━━━━━━━"
                )
            yield event.plain_result("\n".join(report))
        except Exception as e:
            raise RuntimeError(f"删除任务失败: {str(e)}")

    async def _list_tasks(self, event: AstrMessageEvent) -> MessageEventResult:
        """列出当前会话（unified_msg_origin 匹配）的任务"""
        try:
            current_unified = event.unified_msg_origin
            
            current_tasks = [
                t for t in self.tasks 
                if t["unified_msg_origin"] == current_unified
            ]
            
            if not current_tasks:
                yield event.plain_result("当前会话没有定时任务")
                return
                
            task_list = [
                "📅 当前会话定时任务列表",
                f"会话标识：{current_unified}",
                "━━━━━━━━━━━━━━━━"
            ]
            
            for idx, task in enumerate(current_tasks, 1):
                status = self._get_status_text(task)
                
                task_list.append(f"▪️ 任务 {idx}")
                task_list.append(f"名称：{task['script_name']}")
                task_list.append(f"时间：每日 {task['time']}")
                task_list.append(f"状态：{status}")
                task_list.append(f"重试次数：{task['retry_count']}次")
                task_list.append(f"任务ID：{task['task_id']}")
                task_list.append("━━━━━━━━━━━━━━━━")
                
            yield event.plain_result("\n".join(task_list))
        except Exception as e:
            raise RuntimeError(f"列出任务失败: {str(e)}")
    
    def _get_status_text(self, task: Dict) -> str:
        """获取任务状态的文本描述"""
        if task["status"] == TASK_RUNNING:
            if task["task_id"] in self.running_tasks:
                now = datetime.now(CHINA_TZ)
                start_time = self.running_tasks[task["task_id"]]
                run_seconds = (now - start_time).seconds
                return f"🏃‍♂️ 执行中 (已运行 {run_seconds}秒)"
            return "🏃‍♂️ 执行中"
        elif task["status"] == TASK_SUCCEEDED:
            last_run = safe_datetime(task.get("last_run"))
            if last_run:
                return f"✅ 已成功 (于 {last_run.strftime('%m/%d %H:%M')})"
            return "✅ 已成功"
        elif task["status"] == TASK_FAILED:
            return f"❌ 失败 (尝试 {task['retry_count']}次)"
        return "⏳ 待触发"

    async def _task_status(self, event: AstrMessageEvent, task_id: str = None) -> MessageEventResult:
        """查询任务详细状态"""
        try:
            current_unified = event.unified_msg_origin
            
            if task_id:
                # 查找特定任务
                task = next(
                    (t for t in self.tasks if t["unified_msg_origin"] == current_unified and t["task_id"].lower() == task_id.lower()), 
                    None
                )
                if not task:
                    yield event.plain_result(f"找不到任务 ID: {task_id}")
                    return
                    
                status_info = []
                status_info.append(f"📊 任务详细状态: {task['task_id']}")
                status_info.append(f"脚本名称: {task['script_name']}")
                status_info.append(f"状态: {self._get_status_text(task)}")
                status_info.append(f"执行时间: 每日 {task['time']}")
                
                last_attempt = safe_datetime(task.get("last_attempt"))
                if last_attempt:
                    status_info.append(f"最后尝试: {last_attempt.strftime('%Y-%m-%d %H:%M')}")
                    
                last_run = safe_datetime(task.get("last_run"))
                if last_run:
                    status_info.append(f"最后成功执行: {last_run.strftime('%Y-%m-%d %H:%M')}")
                    
                status_info.append(f"重试次数: {task['retry_count']}")
                
                if task["task_id"] in self.running_tasks:
                    start_time = self.running_tasks[task["task_id"]]
                    now = datetime.now(CHINA_TZ)
                    run_seconds = (now - start_time).seconds
                    status_info.append(f"已运行: {run_seconds}秒")
                    
                yield event.plain_result("\n".join(status_info))
            else:
                # 列出所有任务状态（当前会话）
                tasks = [t for t in self.tasks if t["unified_msg_origin"] == current_unified]
                if not tasks:
                    yield event.plain_result("当前会话没有定时任务")
                    return
                    
                status_list = ["📊 当前会话任务状态概览"]
                status_list.append(f"会话标识: {current_unified}")
                status_list.append("ID (前8位) | 脚本名称 | 状态")
                status_list.append("━━━━━━━━━━━━━━━━━━━━━━")
                
                for task in tasks:
                    short_id = task["task_id"][-8:]  # 显示后8位ID
                    script_short = task["script_name"][:10] + ("..." if len(task["script_name"]) > 10 else "")
                    status = self._get_status_text(task)[:15]  # 截短状态描述
                    
                    status_list.append(f"{short_id} | {script_short} | {status}")
                
                # 添加统计信息
                status_list.append("━━━━━━━━━━━━━━━━━━━━━━")
                running_count = sum(1 for t in tasks if t["status"] == TASK_RUNNING)
                pending_count = sum(1 for t in tasks if t["status"] == TASK_PENDING)
                success_count = sum(1 for t in tasks if t["status"] == TASK_SUCCEEDED)
                failed_count = sum(1 for t in tasks if t["status"] == TASK_FAILED)
                
                status_list.append(f"▶️ 运行中: {running_count} | ⏳ 待触发: {pending_count}")
                status_list.append(f"✅ 成功: {success_count} | ❌ 失败: {failed_count}")
                status_list.append("使用 `/定时 状态 [任务ID]` 查看详细状态")
                
                yield event.plain_result("\n".join(status_list))
        except Exception as e:
            raise RuntimeError(f"获取任务状态失败: {str(e)}")

    async def _show_help(self, event: AstrMessageEvent) -> MessageEventResult:
        """显示帮助信息"""
        help_msg = """
📘 定时任务插件使用指南 V5.0.4

【命令列表】
/定时 添加 [脚本名] [时间] - 创建每日定时任务
/定时 删除 [任务ID或名称] - 删除当前会话的任务
/定时 列出 - 查看当前会话的所有任务
/定时 状态 [任务ID] - 查看任务详细执行状态
/定时 帮助 - 显示本帮助信息
/执行 [脚本名] - 立即执行脚本并返回结果

【新特性】
✓ 支持多日任务自动重置
✓ 增强日期变更检测机制
✓ 执行记录持久化存储

【示例】
/定时 添加 数据备份 08:30
/定时 删除 数据备份_0830_12345
/执行 数据备份

⚠️ 注意：
- 任务ID由脚本名+时间+会话ID生成
- 脚本执行超时60秒自动终止
- 仅当前会话的任务可管理
- 每日0点自动重置所有任务状态
        """.strip()
        yield event.plain_result(help_msg)

    async def terminate(self) -> None:
        """插件卸载时停止所有任务"""
        logger.info("插件终止中...")
        
        # 取消检查器任务
        if hasattr(self, "schedule_checker_task"):
            self.schedule_checker_task.cancel()
            try:
                await self.schedule_checker_task
            except asyncio.CancelledError:
                logger.info("定时检查器已终止")
            except Exception:
                pass
                
        # 取消监控器任务
        if hasattr(self, "task_monitor_task"):
            self.task_monitor_task.cancel()
            try:
                await self.task_monitor_task
            except asyncio.CancelledError:
                logger.info("状态监控器已终止")
            except Exception:
                pass
                
        # 保存任务状态和执行记录
        try:
            self._save_tasks()
            self._save_executed_tasks()
        except Exception as e:
            logger.error(f"插件终止时保存失败: {str(e)}")
            
        logger.info("插件终止完成")
