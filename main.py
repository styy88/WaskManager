from astrbot.api.event import filter, AstrMessageEvent, MessageEventResult
from astrbot.api.star import Context, Star, register
from astrbot.api import logger
from astrbot.api.message_components import Plain, Image
from astrbot.api.platform import MessageType
from astrbot.core.utils.io import download_image_by_url
import os
import re
import json
import asyncio
import subprocess
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Union

# 创建UTC+8时区
china_tz = timezone(timedelta(hours=8))

def generate_task_id(task: Dict) -> str:
    """生成唯一任务标识"""
    return f"{task['script_name']}_{task['time'].replace(':', '')}_{task['receiver_type'][0]}_{task['receiver']}"

@register("ZaskManager", "xiaoxin", "全功能定时任务插件", "3.5", "https://github.com/styy88/ZaskManager")
class ZaskManager(Star):
    def __init__(self, context: Context, config: dict = None):
        super().__init__(context)
        self.context = context
        self.config = config or {}
        
        # 路径配置
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
        logger.info(f"插件数据目录初始化完成: {self.plugin_root}")

        self.tasks: List[Dict] = []
        self._load_tasks()
        self.schedule_checker_task = asyncio.create_task(self.schedule_checker())

    def _load_tasks(self):
        """安全加载任务数据"""
        try:
            if os.path.exists(self.tasks_file):
                with open(self.tasks_file, "r", encoding="utf-8") as f:
                    raw_tasks = json.load(f)
                    self.tasks = [
                        {**task, "task_id": task.get("task_id") or generate_task_id(task)}
                        for task in raw_tasks
                        if self._validate_task(task)
                    ]
                logger.info(f"成功加载 {len(self.tasks)} 个有效定时任务")
        except Exception as e:
            logger.error(f"任务加载失败: {str(e)}")
            self.tasks = []

    def _validate_task(self, task: Dict) -> bool:
        """验证任务数据有效性"""
        required_keys = ["script_name", "time", "receiver_type", "receiver", "platform"]
        return all(key in task for key in required_keys)

    def _save_tasks(self):
        """安全保存任务数据"""
        valid_tasks = [task for task in self.tasks if self._validate_task(task)]
        with open(self.tasks_file, "w", encoding="utf-8") as f:
            json.dump(valid_tasks, f, indent=2, ensure_ascii=False)

    async def schedule_checker(self):
        """定时任务检查器"""
        logger.info("定时检查器启动")
        while True:
            try:
                await asyncio.sleep(30 - datetime.now().second % 30)
                now = datetime.now(china_tz)
                current_time = now.strftime("%H:%M")
                
                for task in self.tasks.copy():
                    if task["time"] == current_time and self._should_trigger(task, now):
                        await self._process_task(task, now)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"定时检查器错误: {str(e)}")
                await asyncio.sleep(10)

    def _should_trigger(self, task: Dict, now: datetime) -> bool:
        """判断是否应该触发任务"""
        last_run = datetime.fromisoformat(task["last_run"]).astimezone(china_tz) if task.get("last_run") else None
        return not last_run or (now - last_run).total_seconds() >= 86400

    async def _process_task(self, task: Dict, now: datetime):
        """任务处理流程"""
        try:
            output = await self._execute_script(task["script_name"])
            await self._send_task_result(task, output)
            task["last_run"] = now.isoformat()
            self._save_tasks()
        except Exception as e:
            logger.error(f"任务执行失败: {str(e)}")
            await self._send_error_notice(task, str(e))

    async def _send_error_notice(self, task: Dict, error_msg: str):
        """错误通知处理"""
        await self._send_message(task, [Plain(text=f"❌ 任务执行失败: {error_msg[:500]}")])

    async def _send_task_result(self, task: Dict, message: str):
        """发送任务结果"""
        try:
            await self._send_message(task, [Plain(text=message[:2000])])
        except Exception as e:
            logger.error(f"消息发送失败: {str(e)}")
            raise

    async def _send_message(self, task: Dict, components: list):
        """消息发送"""
        try:
            # 构造目标会话信息
            target = {
                "platform": task['platform'].lower(),
                "receiver_type": task['receiver_type'],
                "receiver_id": task['receiver']
            }
            
            # 处理消息组件
            message_chain = []
            for comp in components:
                if isinstance(comp, Image):
                    if comp.file.startswith("http"):
                        local_path = await download_image_by_url(comp.file)
                        message_chain.append(Image(file=f"file:///{local_path}"))
                    else:
                        message_chain.append(comp)
                else:
                    message_chain.append(comp)

            # 发送
            await self.context.send_message(
                target=target,
                message_chain=message_chain
            )
            logger.debug(f"消息已发送至 {target}")

        except Exception as e:
            logger.error(f"消息发送失败: {str(e)}", exc_info=True)
            raise RuntimeError(f"消息发送失败: {str(e)}")

    async def _execute_script(self, script_name: str) -> str:
        """执行脚本文件（增加安全性）"""
        script_path = os.path.join(self.plugin_root, f"{script_name}.py")
        
        if not os.path.exists(script_path):
            available = ", ".join(f.replace('.py', '') for f in os.listdir(self.plugin_root) if f.endswith('.py'))
            raise FileNotFoundError(f"脚本不存在！可用脚本: {available or '无'}")

        try:
            result = await asyncio.wait_for(
                asyncio.create_subprocess_exec(
                    "python", script_path,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                ),
                timeout=30
            )
            stdout, stderr = await result.communicate()
            
            if result.returncode != 0:
                raise RuntimeError(f"执行失败（代码{result.returncode}）: {stderr.decode('utf-8')}")
                
            return stdout.decode("utf-8")
        except asyncio.TimeoutError:
            raise TimeoutError("执行超时（30秒限制）")
        except Exception as e:
            raise RuntimeError(f"执行错误: {str(e)}")

    @filter.command("定时")
    async def schedule_command(self, event: AstrMessageEvent):
        """处理定时命令"""
        try:
            parts = event.message_str.split(maxsplit=3)
            if len(parts) < 2:
                raise ValueError("命令格式错误，请输入'/定时 帮助'查看用法")

            command = parts[1].lower()
            if command == "添加":
                async for msg in self._add_task(event, parts[2], parts[3]):
                    yield msg
            elif command == "删除":
                async for msg in self._delete_task(event, parts[2]):
                    yield msg
            elif command == "列出":
                async for msg in self._list_tasks(event):
                    yield msg
            else:
                async for msg in self._show_help(event):
                    yield msg

        except Exception as e:
            event.stop_event()
            yield event.plain_result(f"❌ 错误: {str(e)}")

    async def _add_task(self, event: AstrMessageEvent, name: str, time_str: str):
        """添加定时任务"""
        if not re.fullmatch(r"^([01]\d|2[0-3]):([0-5]\d)$", time_str):
            raise ValueError("时间格式应为 HH:MM（24小时制），例如：14:00")

        group_id = event.get_group_id()
        user_id = event.get_sender_id()
        platform = event.get_platform_name().upper()

        script_path = os.path.join(self.plugin_root, f"{name}.py")
        if not os.path.exists(script_path):
            available = ", ".join(f.replace('.py', '') for f in os.listdir(self.plugin_root))
            raise FileNotFoundError(f"脚本不存在！可用脚本: {available or '无'}")

        new_task = {
            "script_name": name,
            "time": time_str,
            "receiver_type": "group" if group_id else "private",
            "receiver": group_id if group_id else user_id,
            "platform": event.get_platform_name().upper(),
            "last_run": None,
            "created": datetime.now(china_tz).isoformat()
        }
        new_task["task_id"] = generate_task_id(new_task)
        
        if any(t["task_id"] == new_task["task_id"] for t in self.tasks):
            raise ValueError(f"该时段任务已存在（ID: {new_task['task_id']}）")
            
        self.tasks.append(new_task)
        self._save_tasks()
    
        reply_msg = (
            "✅ 定时任务创建成功\n"
            f"名称：{name}\n"
            f"时间：每日 {time_str}\n"
            f"绑定到：{'群聊' if new_task['receiver_type'] == 'group' else '私聊'}\n"
            f"平台：{platform}\n"
            f"任务ID：{new_task['task_id']}"
        )
        yield event.plain_result(reply_msg)

    async def _delete_task(self, event: AstrMessageEvent, identifier: str):
        """删除当前会话的任务"""
        group_id = event.get_group_id()
        receiver_type = "group" if group_id else "private"
        receiver = group_id if group_id else event.get_sender_id()
        platform = event.get_platform_name().upper()
        
        current_tasks = [
            t for t in self.tasks 
            if t["receiver_type"] == receiver_type
            and t["receiver"] == receiver
            and t["platform"] == platform
        ]
        
        if not current_tasks:
            yield event.plain_result("当前会话没有定时任务")
            return
            
        deleted = []
        for task in current_tasks.copy():
            if identifier.lower() in (task["task_id"].lower(), task["script_name"].lower()):
                self.tasks.remove(task)
                deleted.append(task)
                
        if not deleted:
            available_ids = "\n".join([f"· {t['task_id']}" for t in current_tasks])
            raise ValueError(f"未找到匹配任务，当前可用ID：\n{available_ids}")
            
        self._save_tasks()
        
        report = ["✅ 已删除以下任务："]
        for task in deleted:
            report.append(
                f"▫️ {task['script_name']} ({task['time']})\n"
                f"  平台：{task['platform']}\n"
                f"  任务ID：{task['task_id']}\n"
                "━━━━━━━━━━━━━━━━"
            )
        yield event.plain_result("\n".join(report))

    async def _list_tasks(self, event: AstrMessageEvent):
        """列出当前会话任务"""
        group_id = event.get_group_id()
        receiver_type = "group" if group_id else "private"
        receiver = group_id if group_id else event.get_sender_id()
        platform = event.get_platform_name().upper()
        
        current_tasks = [
            t for t in self.tasks 
            if t["receiver_type"] == receiver_type
            and t["receiver"] == receiver
            and t["platform"] == platform
        ]
        
        if not current_tasks:
            yield event.plain_result("当前会话没有定时任务")
            return
            
        task_list = [
            "📅 当前会话定时任务列表",
            f"会话类型：{'群聊' if receiver_type == 'group' else '私聊'}",
            f"平台类型：{platform}",
            f"会话ID：{receiver}",
            "━━━━━━━━━━━━━━━━"
        ]
        
        for index, task in enumerate(current_tasks, 1):
            status = "✅ 已激活" if task.get("last_run") else "⏳ 待触发"
            last_run = datetime.fromisoformat(task["last_run"]).strftime("%m/%d %H:%M") if task.get("last_run") else "尚未执行"
            
            task_list.extend([
                f"▪️ 任务 {index}",
                f"名称：{task['script_name']}",
                f"时间：每日 {task['time']}",
                f"状态：{status}",
                f"最后执行：{last_run}",
                f"任务ID：{task['task_id']}",
                "━━━━━━━━━━━━━━━━"
            ])
            
        yield event.plain_result("\n".join(task_list))

    @filter.command("执行")
    async def execute_command(self, event: AstrMessageEvent):
        """处理立即执行命令"""
        try:
            parts = event.message_str.split(maxsplit=1)
            if len(parts) < 2:
                raise ValueError("格式应为：/执行 [脚本名]")

            script_name = parts[1].strip()
            output = await self._execute_script(script_name)
            yield event.plain_result(f"✅ 执行成功\n{output[:1500]}")
        except Exception as e:
            event.stop_event()
            yield event.plain_result(f"❌ 执行错误: {str(e)}")

    async def _show_help(self, event: AstrMessageEvent):
        """显示帮助信息"""
        help_msg = """
📘 定时任务插件使用指南（v3.5+）

【核心功能】
✅ 跨平台支持：微信/QQ/Telegram
✅ 群聊 & 私聊消息
✅ 脚本执行结果自动发送

【命令列表】
/定时 添加 [脚本名] [时间] - 创建新任务
/定时 删除 [任务ID或名称] - 删除任务
/定时 列出 - 显示当前会话任务
/执行 [脚本名] - 立即执行脚本

【示例】
/定时 添加 数据备份 08:30 -> 创建每日备份
/定时 删除 备份任务_0830 -> 移除任务
/执行 生成报表 -> 立即运行脚本

【注意事项】
1. 脚本需放置在 plugin_data/ZaskManager 目录
2. 时间格式为 24小时制（如 14:00）
3. 任务ID在添加成功后显示
        """.strip()
        yield event.plain_result(help_msg)

    async def terminate(self):
        """插件卸载时停止所有任务"""
        if hasattr(self, "schedule_checker_task"):
            self.schedule_checker_task.cancel()
        logger.info("插件已安全卸载")
