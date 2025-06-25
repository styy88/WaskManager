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

# 北京时间时区 (UTC+8)
CHINA_TZ = timezone(timedelta(hours=8))

# 任务执行状态常量
TASK_PENDING = "pending"
TASK_RUNNING = "running"
TASK_FAILED = "failed"
TASK_SUCCEEDED = "succeeded"

def generate_task_id(task: Dict) -> str:
    """安全生成任务ID"""
    platform, msg_type, session_id = task["unified_msg_origin"].split(':', 2)
    script_name = re.sub (r'[^\w]', '_', task ["script_name"])[:20]
    return f"{script_name}_{task['time'].replace(':', '')}_{session_id[:10]}"

def safe_datetime(dt: any) -> Optional[datetime]:
    """安全转换各种格式的时间数据为datetime对象"""
    if isinstance(dt, datetime):
        return dt.astimezone(CHINA_TZ)
    
    if not dt:
        return None
        
    # 尝试不同格式解析
    try:
        # ISO格式解析
        return datetime.fromisoformat(dt).astimezone(CHINA_TZ)
    except:
        pass
        
    try:
        # 年月日时分秒格式（不含毫秒）
        return datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=CHINA_TZ)
    except:
        pass
        
    return None

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
        
        # 启动定时器
        self.schedule_checker_task = asyncio.create_task(self.schedule_checker())
        self.task_monitor_task = asyncio.create_task(self.task_monitor())

    def _load_tasks(self) -> None:
        """安全加载任务，兼容所有时间格式"""
        try:
            if os.path.exists(self.tasks_file):
                with open(self.tasks_file, "r", encoding="utf-8") as f:
                    raw_tasks = json.load(f)
                    
                    # 迁移和修复任务数据
                    self.tasks = []
                    for task in raw_tasks:
                        if "unified_msg_origin" not in task:
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
            else:
                self.tasks = []
                logger.info("任务文件不存在，已初始化空任务列表")
                
            # 立即保存规范化后的数据
            self._save_tasks()
        except Exception as e:
            logger.error(f"任务加载失败: {str(e)}")
            self.tasks = []

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

    async def schedule_checker(self) -> None:
        """定时任务检查器（每30秒轮询一次）"""
        logger.info("定时检查器启动")
        while True:
            try:
                await asyncio.sleep(30 - datetime.now().second % 30)
                now = datetime.now(CHINA_TZ)
                current_time = now.strftime("%H:%M")
                
                # 执行日期范围（当前日期）
                today_start = datetime(now.year, now.month, now.day, tzinfo=CHINA_TZ)
                today_end = today_start + timedelta(days=1)
                
                for task in self.tasks.copy():
                    # 检查状态是否适合执行
                    if task["status"] not in [TASK_PENDING, TASK_FAILED]:
                        continue
                        
                    # 检查时间匹配
                    if task["time"] != current_time:
                        continue
                        
                    # 检查是否今天执行过（考虑各种时间格式）
                    last_run = task.get("last_run")
                    if last_run:
                        # 安全转换所有时间格式为datetime
                        last_run_dt = safe_datetime(last_run)
                        if last_run_dt and today_start <= last_run_dt < today_end:
                            logger.debug(f"任务 {task['task_id']} 今天已执行过，跳过")
                            continue
                    
                    # 获取任务锁
                    task_id = task["task_id"]
                    if task_id not in self.task_locks:
                        self.task_locks[task_id] = asyncio.Lock()
                        
                    # 尝试执行任务
                    if not self.task_locks[task_id].locked():
                        logger.info(f"准备执行任务: {task_id}")
                        asyncio.create_task(self._execute_task_with_lock(task, now))
                    else:
                        logger.warning(f"任务 {task_id} 已在执行中，跳过重复触发")
                
            except asyncio.CancelledError:
                logger.info("定时检查器被取消")
                break
            except Exception as e:
                logger.error(f"定时检查器错误: {str(e)}")
                await asyncio.sleep(10)

    async def task_monitor(self) -> None:
        """任务状态监控器（每6小时检查一次）"""
        logger.info("任务状态监控器启动")
        while True:
            try:
                await asyncio.sleep(21600)  # 6小时
                now = datetime.now(CHINA_TZ)
                today_start = datetime(now.year, now.month, now.day, tzinfo=CHINA_TZ)
                
                # 检查未执行的任务（没有今天执行记录）
                missed_tasks = [
                    task for task in self.tasks
                    if task["status"] != TASK_SUCCEEDED and 
                    (not task.get("last_run") or safe_datetime(task["last_run"]) < today_start)
                ]
                
                # 报告错过的任务
                if missed极asks:
                    report = []
                    
                    for task in missed_tasks:
                        # 获取任务时间对象
                        task_time_dt = safe_datetime(task.get("last_run"))
                        
                        # 格式化状态信息
                        status = task["status"]
                        if status == TASK_RUNNING:
                            status = "运行中"
                        elif status == TASK_FAILED:
                            status = f"失败({task.get('retry_count',0)}次)"
                        else:
                            status = "待执行"
                            
                        # 格式化最后执行时间
                        last_run = task_time_dt.strftime("%Y-%m-%d %H:%M") if task_time_dt else "从未执行"
                        
                        # 获取任务标识
                        task_ident = task["task_id"][:15]
                        
                        report.append(f"• [{status}] {task['script_name']} - {last_run} ({task_ident})")
                    
                    # 只显示前5个错过的任务避免过长
                    if len(report) > 5:
                        report = report[:5] + [f"... 共{len(missed_tasks)}个任务错过执行"]
                    
                    # 构造消息
                    now_str = now.strftime("%Y-%m-%d %H:%M")
                    message = f"⚠️ 定时任务状态监控\n⏰ 系统时间: {now_str}\n📋 今日未成功执行的任务:\n" + "\n".join(report)
                    
                    # 发给第一个任务所属的会话
                    if self.tasks:
                        first_task = self.tasks[0]
                        await self.context.send_message(
                            first_task["unified_msg_origin"],
                            MessageChain([Plain(text=message)])
                        )
                
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
                self._save_tasks()

    async def _execute_task_with_lock(self, task: Dict, now: datetime) -> None:
        """带锁执行任务（防止并发）"""
        task_id = task["task_id"]
        task_lock = self.task_locks[task_id]
        
        async with task_lock:
            try:
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
                except:
                    logger.error("错误通知发送失败")
                    
            finally:
                # 清理状态
                self.running_tasks.pop(task_id, None)
                self._save_tasks()

    async def _execute_script(self, script_name: str) -> str:
        """执行指定脚本并返回输出"""
        script_path = os.path.join(self.plugin_root, f"{script_name}.py")
        
        # 检查脚本是否存在
        if not os.path.exists(script_path):
            available = [f.replace('.py', '') for f in os.listdir(self.plugin_root) if f.endswith('.py')]
            raise FileNotFoundError(f"脚本不存在！可用脚本: {', '.join(available) or '无'}")
        
        try:
            logger.info(f"立即执行脚本: {script_path}")
            proc = await asyncio.create_subprocess_exec(
                "python", script_path,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            
            # 等待执行完成
            try:
                stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=30)
            except asyncio.TimeoutError:
                proc.kill()
                await proc.communicate()
                raise TimeoutError("脚本执行超时 (30秒)")
            
            # 检查执行结果
            if proc.returncode != 0:
                error_msg = stderr.decode("utf-8", "ignore")[:500]
                raise RuntimeError(f"执行失败 (代码 {proc.returncode}): {error_msg}")
            
            # 返回输出
            return stdout.decode("utf-8", "ignore")
        except Exception as e:
            raise RuntimeError(f"执行错误: {str(e)}")

    async def _send_task_result(self, task: Dict, message: str) -> None:
        """发送任务结果"""
        try:
            if not message.strip():
                message = f"✅ 脚本 {task['script_name']} 执行成功，无内容输出"
                
            # 添加任务信息前缀
            result_text = f"⏰ 定时任务报告\n🖥️ {task['script_name']}\n➡️ 输出内容:\n{message}"
            
            # 限长防止消息过长
            if len(result_text) > 1500:
                result_text = result_text[:1400] + "... [输出过长被截断]"
                
            message_chain = MessageChain([Plain(text=result_text)])
            await self.context.send_message(
                task["unified_msg_origin"],
                message_chain
            )
        except Exception as e:
            logger.error(f"消息发送失败: {str(e)}")

    # 命令处理器（增强错误处理）
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
            
    @filter.command("执行")
    async def execute_command(self, event: AstrMessageEvent) -> MessageEventResult:
        """立即执行脚本命令"""
        try:
            # 获取脚本名
            script_name = event.message_str.strip().split(maxsplit=1)
            if len(script_name) < 2:
                raise ValueError("格式错误，应为: /执行 [脚本名]")
                
            script_name = script_name[1]
            
            # 执行脚本
            result = await self._execute_script(script_name)
            
            # 返回结果
            if len(result) > 1500:
                result = result[:1400] + "... [输出过长被截断]"
                
            yield event.plain_result(f"🖥️ {script_name} 执行结果:\n{result}")
            
        except Exception as e:
            yield event.plain_result(f"❌ 脚本执行失败: {str(e)}")
            
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
            "created": datetime.now(CHINA_TZ)
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
                now = datetime.now(CHINA_TZ)
                run_seconds = (now - self.running_tasks[task["task_id"]]).seconds
                running_time = f" (已运行 {run_seconds}秒)"
            return f"🏃‍♂️ 执行中{running_time}"
        elif task["status"] == TASK_SUCCEEDED:
            if task.get("last_run"):
                last_run = task["last_run"]
                if isinstance(last_run, str):
                    last_run = safe_datetime(last_run)
                if last_run:
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
                f"最后执行: {safe_datetime(task.get('last_run')).strftime('%Y-%m-%d %H:%M') if task.get('last_run') else '从未执行'}",
                f"最后尝试: {safe_datetime(task.get('last_attempt')).strftime('%Y-%m-%d %H:%M') if task.get('last_attempt') else '从未尝试'}",
                f"重试次数: {task['retry_count']}",
                f"执行时间: {task['time']}",
            ]
            
            if task_id in self.running_tasks:
                start_time = self.running_tasks[task_id]
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
📘 定时任务插件使用指南 V5.1 (稳定版)

【命令列表】
/定时 添加 [脚本名] [时间] - 创建每日定时任务（脚本需放在 plugin_data/ZaskManager 下）
/定时 删除 [任务ID或名称] - 删除当前会话的任务
/定时 列出 - 查看当前会话的所有任务
/定时 状态 [任务极 查看任务详细执行状态
/定时 帮助 - 显示本帮助信息
/执行 [脚本名] - 立即执行脚本并返回结果

【任务状态】
✅ 成功 - 任务已成功执行
❌ 失败 - 任务执行失败
⏳ 待触发 - 任务等待执行
🏃‍♂️ 执行中 - 任务正在执行

【可靠性保证】
1. 自动重试机制（失败后最多重试3次）
2. 任务卡死检测（5分钟未完成自动恢复）
3. 时间格式自动修复（兼容各种时间格式）
4. 每日状态监控（6小时检查一次任务执行情况）

【示例】
/定时 添加 数据备份 08:30   # 每日08:30执行"数据备份.py"
/定时 删除 数据备份_0830_12345  # 通过任务ID精准删除
/定时 列出                # 查看当前会话所有任务
/定时 状态 data123       # 查看任务详细状态
/执行 数据备份            # 立即运行"数据备份.py"

⚠️ 注意：
- 任务ID由「脚本名+时间+会话ID」生成，添加后可查看
- 定时任务每日执行一次，脚本执行超时30秒会自动终止
- 仅当前会话（群/私聊）的任务会被列出/删除
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
                
        # 取消监控器任务
        if hasattr(self, "task_monitor_task"):
            self.task_monitor_task.cancel()
            try:
                await self.task_monitor_task
            except asyncio.CancelledError:
                logger.info("状态监控器已终止")
                
        logger.info("插件终止完成")
