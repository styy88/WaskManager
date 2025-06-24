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
from typing import List, Dict, Optional

# 时区配置（确保整个系统使用统一时区）
CHINA_TZ = timezone(timedelta(hours=8))

# 任务执行状态常量
TASK_PENDING = "pending"
TASK_RUNNING = "running"
TASK_FAILED = "failed"
TASK_SUCCEEDED = "succeeded"

def generate_task_id(task: Dict) -> str:
    """基于平台+消息类型+会话ID+脚本名+时间生成唯一ID"""
    platform, msg_type, session_id = task["unified_msg_origin"].split(':', 2)
    script_name = re.sub(r'[^\w]', '_', task["script_name"])[:20]
    return f"{script_name}_{task['time'].replace(':', '')}_{session_id[:10]}"

@register("ZaskManager", "xiaoxin", "全功能定时任务插件", "4.1", "https://github.com/styy88/ZaskManager")
class ZaskManager(Star):
    def __init__(self, context: Context, config: dict = None):
        super().__init__(context)
        self.config = config or {}
        
        # 插件数据目录
        self.plugin_root = os.path.abspath(
            os.path.join(
                os.path.dirname(__file__),
                "..", "..",
                "plugin_data",
                "ZaskManager"
            )
        )
        self.tasks_file = os.path.join(self.plugin_root, "tasks.json")
        os.makedirs(self.plugin_root, exist_ok=True)

        self.tasks: List[Dict] = []
        self._load_tasks()
        
        # 状态管理
        self.task_locks = {}  # 任务ID: 锁对象
        self.running_tasks = {}  # 任务ID: 任务开始时间
        self.missed_tasks = []  # 需要补偿执行的任务
        
        # 启动定时器
        self.schedule_checker_task = asyncio.create_task(self.schedule_checker())
        self.task_monitor_task = asyncio.create_task(self.task_monitor())
        self.missed_task_task = asyncio.create_task(self.missed_task_handler())

    def _load_tasks(self) -> None:
        """安全加载任务"""
        try:
            if os.path.exists(self.tasks_file):
                with open(self.tasks_file, "r", encoding="utf-8") as f:
                    raw_tasks = json.load(f)
                    
                    # 迁移和修复任务数据
                    self.tasks = []
                    for task in raw_tasks:
                        if "unified_msg_origin" not in task:
                            continue
                            
                        # 修复时间字段
                        for time_field in ["last_run", "last_attempt", "created"]:
                            if task.get(time_field) and isinstance(task[time_field], str):
                                try:
                                    # 解析为带时区的datetime对象
                                    dt = datetime.fromisoformat(task[time_field])
                                    if dt.tzinfo is None:
                                        dt = dt.replace(tzinfo=CHINA_TZ)
                                    task[time_field] = dt.astimezone(CHINA_TZ)
                                except:
                                    task[time_field] = None
                        
                        # 生成任务ID
                        task_id = generate_task_id(task)
                            
                        # 验证并标准化任务
                        self.tasks.append({
                            "task_id": task_id,
                            "script_name": task["script_name"],
                            "time": task["time"],
                            "unified_msg_origin": task["unified_msg_origin"],
                            "status": task.get("status", TASK_PENDING),
                            "last_run": task.get("last_run"),
                            "last_attempt": task.get("last_attempt"),
                            "retry_count": task.get("retry_count", 0),
                            "created": task.get("created") or datetime.now(CHINA_TZ)
                        })
                    
                logger.info(f"成功加载 {len(self.tasks)} 个有效任务")
            else:
                self.tasks = []
                logger.info("任务文件不存在，已初始化空任务列表")
                
            # 立即保存修复后的数据
            self._save_tasks()
        except Exception as e:
            logger.error(f"任务加载失败: {str(e)}")
            self.tasks = []

    def _save_tasks(self) -> None:
        """安全保存任务，时间字段转为字符串"""
        try:
            with open(self.tasks_file, "w", encoding="utf-8") as f:
                tasks_to_save = []
                for task in self.tasks:
                    # 复制任务数据，转换时间对象
                    save_task = task.copy()
                    for field in ["last_run", "last_attempt", "created"]:
                        if save_task.get(field) and isinstance(save_task[field], datetime):
                            save_task[field] = save_task[field].isoformat()
                    
                    tasks_to_save.append(save_task)
                
                json.dump(tasks_to_save, f, indent=2, ensure_ascii=False)
            logger.debug("任务数据已持久化")
        except Exception as e:
            logger.error(f"任务保存失败: {str(e)}")

    async def schedule_checker(self) -> None:
        """定时任务检查器（每30秒轮询一次）"""
        logger.info("定时检查器启动")
        while True:
            try:
                await asyncio.sleep(30 - datetime.now().second % 30)
                now = datetime.now(CHINA_TZ)
                current_time = now.strftime("%H:%M")
                today = now.date()
                
                for task in self.tasks.copy():
                    if task["status"] not in [TASK_PENDING, TASK_FAILED]:
                        continue
                        
                    if task["time"] != current_time:
                        continue
                        
                    # 检查是否需要触发
                    if not task.get("last_run") or task["last_run"].date() != today:
                        task_id = task["task_id"]
                        
                        # 创建锁对象（如果不存在）
                        if task_id not in self.task_locks:
                            self.task_locks[task_id] = asyncio.Lock()
                        
                        # 尝试获取锁（防止重复执行）
                        if not self.task_locks[task_id].locked():
                            asyncio.create_task(self._execute_task_with_lock(task, now))
                        else:
                            logger.warning(f"任务 {task_id} 已在执行中，跳过重复触发")
                    else:
                        logger.debug(f"任务 {task['task_id']} 今天已执行过，跳过")
                
                # 每小时检查一次卡住的任务
                if now.minute == 0:
                    await self._check_stuck_tasks()
                
            except asyncio.CancelledError:
                logger.info("定时检查器被取消")
                break
            except Exception as e:
                logger.error(f"定时检查器错误: {str(e)}")
                await asyncio.sleep(10)

    async def missed_task_handler(self) -> None:
        """补偿未执行任务处理器（每30秒检查一次）"""
        logger.info("补偿执行任务启动")
        while True:
            try:
                await asyncio.sleep(30)
                now = datetime.now(CHINA_TZ)
                today = now.date()
                
                # 检查每个任务是否错过执行
                for task in self.tasks.copy():
                    if task["status"] in [TASK_RUNNING, TASK_SUCCEEDED]:
                        continue
                        
                    # 解析任务时间
                    try:
                        hour, minute = map(int, task["time"].split(":"))
                        task_time = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
                    except Exception:
                        continue
                    
                    # 检查任务是否应该在今天执行
                    if task.get("last_run") and task["last_run"].date() == today:
                        continue  # 今天已执行过
                    
                    # 计算时间差（任务时间与当前时间）
                    time_diff = (now - task_time).total_seconds()
                    
                    # 条件1：过了任务时间30秒以上
                    # 条件2：任务本应执行但未执行
                    if 30 < time_diff < 86400:  # 在一天内的时间窗口
                        task_id = task["task_id"]
                        
                        # 确保任务不在运行中
                        if task_id in self.running_tasks:
                            continue
                            
                        # 确保没有锁占用
                        if task_id not in self.task_locks:
                            self.task_locks[task_id] = asyncio.Lock()
                            
                        if not self.task_locks[task_id].locked():
                            logger.warning(f"检测到任务 {task_id} 可能错过执行，尝试补偿执行")
                            asyncio.create_task(self._execute_task_with_lock(task, now))
                
                # 清理已完成的任务
                if self.missed_tasks:
                    for task_id in self.missed_tasks.copy():
                        task = next((t for t in self.tasks if t["task_id"] == task_id), None)
                        if task and task["status"] == TASK_SUCCEEDED:
                            self.missed_tasks.remove(task_id)
                    logger.debug(f"清理后待补偿任务数: {len(self.missed_tasks)}")
                
            except asyncio.CancelledError:
                logger.info("补偿执行任务被取消")
                break
            except Exception as e:
                logger.error(f"补偿执行任务错误: {str(e)}")
                await asyncio.sleep(30)

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
                self._save_tasks()
                
                # 添加到补偿列表
                self.missed_tasks.append(task_id)

    async def _execute_task_with_lock(self, task: Dict, now: datetime) -> None:
        """带锁执行任务（防止并发）"""
        task_id = task["task_id"]
        task_lock = self.task_locks[task_id]
        
        async with task_lock:
            try:
                # 更新任务状态
                task["status"] = TASK_RUNNING
                task["last_attempt"] = now
                self.running_tasks[task_id] = now
                
                # 执行脚本
                script_name = task["script_name"]
                logger.info(f"执行脚本: {script_name}")
                
                # 创建脚本执行路径
                script_path = os.path.join(self.plugin_root, f"{script_name}.py")
                
                # 执行脚本（带超时处理）
                proc = await asyncio.create_subprocess_exec(
                    "python", script_path,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )
                
                try:
                    stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=30)
                except asyncio.TimeoutError:
                    # 处理超时
                    proc.kill()
                    await proc.communicate()  # 清理残留
                    raise TimeoutError("执行超时（30秒限制）")
                
                # 检查返回码
                if proc.returncode != 0:
                    error_msg = stderr.decode("utf-8", errors="replace")[:500]
                    raise RuntimeError(f"执行失败（代码{proc.returncode}）: {error_msg}")
                
                output = stdout.decode("utf-8", errors="replace")
                
                # 发送结果
                await self._send_task_result(task, output)
                
                # 更新状态
                task["last_run"] = now
                task["status"] = TASK_SUCCEEDED
                task["retry_count"] = 0
                
                # 如果是从补偿列表执行的，移除它
                if task_id in self.missed_tasks:
                    self.missed_tasks.remove(task_id)
                
            except Exception as e:
                logger.error(f"任务执行失败: {str(e)}")
                task["status"] = TASK_FAILED
                task["retry_count"] = min(task.get("retry_count", 0) + 1, 3)
                
                # 添加到补偿列表（最多重试3次）
                if task["retry_count"] <= 3 and task_id not in self.missed_tasks:
                    self.missed_tasks.append(task_id)
                    
            finally:
                # 清理状态
                self.running_tasks.pop(task_id, None)
                self._save_tasks()

    async def task_monitor(self) -> None:
        """任务状态监控器（每6小时检查一次）"""
        logger.info("任务状态监控器启动")
        while True:
            try:
                await asyncio.sleep(21600)  # 6小时
                now = datetime.now(CHINA_TZ)
                
                # 检查未执行的任务（连续3次失败）
                failed_tasks = []
                stale_tasks = []
                
                for task in self.tasks:
                    # 检查失败任务
                    if task["status"] == TASK_FAILED and task["retry_count"] >= 3:
                        failed_tasks.append(task)
                    
                    # 检查陈旧任务（超过24小时未成功）
                    last_run = task.get("last_run")
                    if last_run:
                        time_diff = (now - last_run).total_seconds()
                        if time_diff > 86400:
                            stale_tasks.append(task)
                
                # 报告异常状态
                if failed_tasks or stale_tasks:
                    report = ["⚠️ 定时任务系统监控报告"]
                    
                    if failed_tasks:
                        report.append("连续失败的任务:")
                        for task in failed_tasks:
                            last_attempt = task.get("last_attempt")
                            last_run_time = last_attempt.strftime("%m-%d %H:%M") if last_attempt else "从未尝试"
                            report.append(f"· {task['script_name']} ({task['time']}) 最后尝试: {last_run_time}")
                    
                    if stale_tasks:
                        report.append("超过24小时未成功执行的任务:")
                        for task in stale_tasks:
                            last_run_time = task['last_run'].strftime("%Y-%m-%d %H:%M") if task.get("last_run") else "从未成功"
                            report.append(f"· {task['script_name']} ({task['time']}) 最后成功执行: {last_run_time}")
                    
                    # 发给第一个任务所属的会话
                    if self.tasks:
                        first_task = self.tasks[0]
                        await self.context.send_message(
                            first_task["unified_msg_origin"],
                            MessageChain([Plain(text="\n".join(report))])
                        )
                
            except asyncio.CancelledError:
                logger.info("任务监控器被取消")
                break
            except Exception as e:
                logger.error(f"任务监控器错误: {str(e)}")
                await asyncio.sleep(3600)

    async def _send_task_result(self, task: Dict, message: str) -> None:
        """发送任务结果"""
        try:
            if not message.strip():
                message = f"✅ 脚本 {task['script_name']} 已执行，无输出"
                
            message_chain = MessageChain([Plain(text=message[:2000])])
            await self.context.send_message(
                task["unified_msg_origin"],
                message_chain
            )
        except Exception as e:
            logger.error(f"消息发送失败: {str(e)}")

    async def _execute_script(self, script_name: str) -> str:
        """执行指定Python脚本（带超时/错误处理）"""
        script_path = os.path.join(self.plugin_root, f"{script_name}.py")
        
        # 检查脚本是否存在
        if not os.path.exists(script_path):
            available_scripts = ", ".join(
                f.replace('.py', '') 
                for f in os.listdir(self.plugin_root) 
                if f.endswith('.py')
            )
            raise FileNotFoundError(f"脚本不存在！可用脚本: {available_scripts or '无'}")

        # 执行脚本（30秒超时）
        try:
            logger.info(f"执行脚本: {script_path}")
            result = await asyncio.wait_for(
                asyncio.create_subprocess_exec(
                    "python", 
                    script_path,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                ),
                timeout=30
            )
            stdout, stderr = await result.communicate()
            
            if result.returncode != 0:
                error_msg = stderr.decode("utf-8", errors="replace")[:500]
                raise RuntimeError(f"执行失败（代码{result.returncode}）: {error_msg}")
            
            return stdout.decode("utf-8", errors="replace")
        except asyncio.TimeoutError:
            raise TimeoutError("执行超时（30秒限制）")
        except Exception as e:
            raise RuntimeError(f"执行错误: {str(e)}")

    @filter.command("定时")
    async def schedule_command(self, event: AstrMessageEvent) -> MessageEventResult:
        """处理「定时」指令（添加/删除/列出/帮助）"""
        try:
            parts = event.message_str.split(maxsplit=3)
            if len(parts) < 2:
                raise ValueError("命令格式错误，请输入 `/定时 帮助` 查看用法")

            cmd = parts[1]
            if cmd == "添加":
                if len(parts) != 4:
                    raise ValueError("格式应为：/定时 添加 [脚本名] [时间]")
                async for msg in self._add_task(event, parts[2], parts[3]):
                    yield msg
                    
            elif cmd == "删除":
                if len(parts) != 3:
                    raise ValueError("格式应为：/定时 删除 [任务ID或名称]")
                async for msg in self._delete_task(event, parts[2]):
                    yield msg
                    
            elif cmd == "列出":
                async for msg in self._list_tasks(event):
                    yield msg

            elif cmd == "状态":
                task_id = parts[2] if len(parts) >= 3 else None
                async for msg in self._task_status(event, task_id):
                    yield msg
                    
            else:
                async for msg in self._show_help(event):
                    yield msg

        except Exception as e:
            yield event.plain_result(f"❌ 错误: {str(e)}")
            
    @filter.command("执行")
    async def execute_command(self, event: AstrMessageEvent) -> MessageEventResult:
        """处理「执行」指令（立即运行脚本）"""
        try:
            parts = event.message_str.split(maxsplit=1)
            if len(parts) < 2:
                raise ValueError("格式应为：/执行 [脚本名]")
                
            script_name = parts[1]
            output = await self._execute_script(script_name)
            
            # 直接返回脚本输出，不加任何前缀
            yield event.plain_result(output[:1500])
            
        except Exception as e:
            yield event.plain_result(f"❌ 错误: {str(e)}")

    async def _add_task(self, event: AstrMessageEvent, name: str, time_str: str) -> MessageEventResult:
        """添加定时任务（绑定当前会话的 unified_msg_origin）"""
        if not name or not time_str:
            raise ValueError("参数不能为空，格式：/定时 添加 [脚本名] [时间]")
        
        # 验证时间格式（HH:MM）
        if not re.fullmatch(r"^([01]\d|2[0-3]):([0-5]\d)$", time_str):
            raise ValueError("时间格式应为 HH:MM（24小时制），例如：14:00")

        # 获取当前会话的唯一标识
        unified_msg_origin = event.unified_msg_origin

        # 检查脚本存在性
        script_path = os.path.join(self.plugin_root, f"{name}.py")  
        if not os.path.exists(script_path):
            available = ", ".join(f.replace('.py', '') for f in os.listdir(self.plugin_root) if f.endswith('.py'))
            raise FileNotFoundError(f"脚本不存在！可用脚本: {available or '无'}")

        # 构建新任务
        new_task = {
            "script_name": name,
            "time": time_str,
            "unified_msg_origin": unified_msg_origin,
            "last_run": None,
            "last_attempt": None,
            "status": TASK_PENDING,
            "retry_count": 0,
            "created": datetime.now(timezone(timedelta(hours=8))).isoformat()
        }
        new_task["task_id"] = generate_task_id(new_task)  # 生成唯一ID
        
        # 检查同一会话、同脚本、同时间的任务冲突
        conflicting = any(
            t["unified_msg_origin"] == new_task["unified_msg_origin"] 
            and t["script_name"] == new_task["script_name"] 
            and t["time"] == new_task["time"]
            for t in self.tasks
        )
        if conflicting:
            raise ValueError(f"同一会话下，{name} 脚本在 {time_str} 的任务已存在")
            
        self.tasks.append(new_task)
        self._save_tasks()
    
        # 构造回复
        reply_msg = (
            "✅ 定时任务创建成功\n"
            f"名称：{name}\n"
            f"时间：每日 {time_str}\n"
            f"会话标识：{unified_msg_origin}\n"
            f"任务ID：{new_task['task_id']}\n"
            f"状态：{self._get_status_text(new_task)}"
        )
        yield event.plain_result(reply_msg)

    async def _delete_task(self, event: AstrMessageEvent, identifier: str) -> MessageEventResult:
        """删除当前会话（unified_msg_origin 匹配）的任务"""
        current_unified = event.unified_msg_origin  # 当前会话标识
        
        current_tasks = [
            t for t in self.tasks 
            if t["unified_msg_origin"] == current_unified
        ]
        
        if not current_tasks:
            yield event.plain_result("当前会话没有定时任务")
            return
            
        deleted_tasks = []
        for task in current_tasks.copy():
            if identifier in (task["task_id"], task["script_name"]):
                # 如果任务正在运行，尝试取消
                task_id = task["task_id"]
                if task_id in self.running_tasks:
                    logger.warning(f"尝试取消正在运行的任务: {task_id}")
                    # 标记为已取消（虽然实际上脚本还在后台运行，但不再报告）
                    self.running_tasks.pop(task_id, None)
                    
                self.tasks.remove(task)
                deleted_tasks.append(task)
                
        if not deleted_tasks:
            available_ids = "\n".join([f"· {t['task_id']}" for t in current_tasks])
            raise ValueError(f"未找到匹配任务，当前可用ID：\n{available_ids}")
            
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

    async def _list_tasks(self, event: AstrMessageEvent) -> MessageEventResult:
        """列出当前会话（unified_msg_origin 匹配）的任务"""
        current_unified = event.unified_msg_origin  # 当前会话标识
        
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
            
            task_list.extend([
                f"▪️ 任务 {idx}",
                f"名称：{task['script_name']}",
                f"时间：每日 {task['time']}",
                f"状态：{status}",
                f"重试次数：{task['retry_count']}次",
                f"任务ID：{task['task_id']}",
                "━━━━━━━━━━━━━━━━"
            ])
            
        yield event.plain_result("\n".join(task_list))
    
    def _get_status_text(self, task: Dict) -> str:
        """获取任务状态的文本描述"""
        if task["status"] == TASK_RUNNING:
            running_time = ""
            if task["task_id"] in self.running_tasks:
                now = datetime.now(timezone(timedelta(hours=8)))
                run_seconds = (now - self.running_tasks[task["task_id"]]).seconds
                running_time = f" (已运行 {run_seconds}秒)"
            return f"🏃‍♂️ 执行中{running_time}"
        elif task["status"] == TASK_SUCCEEDED:
            if task.get("last_run"):
                last_run = datetime.fromisoformat(task["last_run"])
                return f"✅ 已成功 (于 {last_run.strftime('%m/%d %H:%M')})"
            return "✅ 已成功"
        elif task["status"] == TASK_FAILED:
            return f"❌ 失败 (尝试 {task['retry_count']}次)"
        return "⏳ 待触发"

    async def _task_status(self, event: AstrMessageEvent, task_id: str = None) -> MessageEventResult:
        """查询任务详细状态"""
        current_unified = event.unified_msg_origin
        
        if task_id:
            # 查找特定任务
            task = next(
                (t for t in self.tasks if t["unified_msg_origin"] == current_unified and t["task_id"] == task_id), 
                None
            )
            if not task:
                yield event.plain_result(f"找不到任务 ID: {task_id}")
                return
                
            status_info = [
                f"📊 任务详细状态: {task_id}",
                f"脚本名称: {task['script_name']}",
                f"状态: {self._get_status_text(task)}",
                f"最后执行: {task['last_run'] or '从未执行'}",
                f"最后尝试: {task['last_attempt'] or '从未尝试'}",
                f"重试次数: {task['retry_count']}",
                f"执行时间: {task['time']}",
            ]
            
            if task_id in self.running_tasks:
                start_time = self.running_tasks[task_id]
                now = datetime.now(timezone(timedelta(hours=8)))
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
            status_list.append("━ ID ━━ 名称 ━━ 状态 ━━━━ 最后执行")
            
            for task in tasks:
                short_id = task["task_id"][-8:]  # 显示后8位ID更易识别
                status = self._get_status_text(task).replace("\n", " ")
                status_line = f" {short_id} | {task['script_name']} | {status}"
                status_list.append(status_line[:60])  # 限制行长度
            
            # 添加统计信息
            running_count = sum(1 for t in tasks if t["status"] == TASK_RUNNING)
            pending_count = sum(1 for t in tasks if t["status"] == TASK_PENDING)
            success_count = sum(1 for t in tasks if t["status"] == TASK_SUCCEEDED)
            failed_count = sum(1 for t in tasks if t["status"] == TASK_FAILED)
            
            status_list.append("━━━━━━━━━━━━━━━━")
            status_list.append(f"▶️ 运行中: {running_count} | ⏳ 待触发: {pending_count}")
            status_list.append(f"✅ 成功: {success_count} | ❌ 失败: {failed_count}")
            status_list.append("使用 `/定时 状态 [任务ID]` 查看详细状态")
            
            yield event.plain_result("\n".join(status_list))

    async def _show_help(self, event: AstrMessageEvent) -> MessageEventResult:
        """显示帮助信息"""
        help_msg = """
📘 定时任务插件使用指南 V3.6 (增强版)

【命令列表】
/定时 添加 [脚本名] [时间] - 创建每日定时任务（脚本需放在 plugin_data/ZaskManager 下）
/定时 删除 [任务ID或名称] - 删除当前会话的任务
/定时 列出 - 查看当前会话的所有任务
/定时 状态 [任务ID] - 查看任务详细执行状态
/定时 帮助 - 显示本帮助信息
/执行 [脚本名] - 立即执行脚本并返回结果

【任务状态】
✅ 成功 - 任务已成功执行
❌ 失败 - 任务执行失败
⏳ 待触发 - 任务等待执行
🏃‍♂️ 执行中 - 任务正在执行

【可靠性增强】
1. 任务自动重试机制（失败后自动重试2次）
2. 任务卡死检测（5分钟未完成自动恢复）
3. 任务状态监控（失败和长时间未成功任务自动提醒）

【示例】
/定时 添加 数据备份 08:30   # 每日08:30执行"数据备份.py"
/定时 删除 数据备份_0830_12345  # 通过任务ID精准删除
/定时 列出                # 查看当前会话所有任务
/定时 状态 data123       # 查看任务详细状态
/执行 数据备份            # 立即运行"数据备份.py"

🛑 注意：
- 任务ID由「脚本名+时间+会话ID」生成，添加后可查看
- 定时任务每日执行一次，脚本执行超时30秒会自动终止
- 仅当前会话（群/私聊）的任务会被列出/删除
- 任务状态自动监控，异常情况会发送提醒
        """.strip()
        yield event.plain_result(help_msg)

    async def terminate(self) -> None:
        """插件卸载时停止定时检查任务"""
        logger.info("插件终止中...")
        if hasattr(self, "schedule_checker_task"):
            self.schedule_checker_task.cancel()
            try:
                await self.schedule_checker_task
            except asyncio.CancelledError:
                logger.info("定时任务已取消")
                
        if hasattr(self, "task_monitor_task"):
            self.task_monitor_task.cancel()
            try:
                await self.task_monitor_task
            except asyncio.CancelledError:
                logger.info("任务监控器已取消")
                
        if hasattr(self, "missed_task_task"):
            self.missed_task_task.cancel()
            try:
                await self.missed_task_task
            except asyncio.CancelledError:
                logger.info("补偿任务处理器已取消")
                
        logger.info("插件终止完成")
