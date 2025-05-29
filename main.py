from astrbot.api.event import filter, AstrMessageEvent, MessageEventResult, MessageChain
from astrbot.api.star import Context, Star, register
from astrbot.api import logger
from astrbot.api.message_components import Plain  # 文本消息组件
from astrbot.api.platform import MessageType     # 消息类型枚举（按需）
from datetime import datetime, timedelta, timezone
import os
import re
import json
import asyncio
import subprocess
from typing import List, Dict, Optional

def generate_task_id(task: Dict) -> str:
    """基于「平台+消息类型+会话ID+脚本名+时间」生成唯一任务标识"""
    platform, msg_type, session_id = task["unified_msg_origin"].split(':', 2)
    return f"{task['script_name']}_{task['time'].replace(':', '')}_{session_id}"


@register("ZaskManager", "xiaoxin", "全功能定时任务插件", "3.5", "https://github.com/styy88/ZaskManager")
class ZaskManager(Star):
    def __init__(self, context: Context, config: dict = None):
        super().__init__(context)
        self.config = config or {}
        
        # 插件数据目录（持久化任务/脚本，遵循 data 目录规范）
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
        logger.debug(f"插件数据目录初始化完成: {self.plugin_root}")

        self.tasks: List[Dict] = []
        self._load_tasks()  # 加载历史任务
        self.schedule_checker_task = asyncio.create_task(self.schedule_checker())

    def _load_tasks(self) -> None:
        """安全加载任务（过滤旧格式数据，确保含 unified_msg_origin）"""
        try:
            if not os.path.exists(self.plugin_root):
                os.makedirs(self.plugin_root, exist_ok=True)
                logger.warning(f"自动创建缺失目录: {self.plugin_root}")
            
            if os.path.exists(self.tasks_file):
                with open(self.tasks_file, "r", encoding="utf-8") as f:
                    raw_tasks = json.load(f)
                    self.tasks = [
                        {**task, "task_id": task.get("task_id") or generate_task_id(task)}
                        for task in raw_tasks
                        if "unified_msg_origin" in task  # 仅加载新格式任务
                    ]
                logger.info(f"成功加载 {len(self.tasks)} 个有效任务")
            else:
                self.tasks = []
                logger.info("任务文件不存在，已初始化空任务列表")
        except Exception as e:
            logger.error(f"任务加载失败: {str(e)}")
            self.tasks = []

    def _save_tasks(self) -> None:
        """安全保存任务到本地文件"""
        try:
            with open(self.tasks_file, "w", encoding="utf-8") as f:
                json.dump(self.tasks, f, indent=2, ensure_ascii=False)
            logger.debug("任务数据已持久化")
        except Exception as e:
            logger.error(f"任务保存失败: {str(e)}")

    async def schedule_checker(self) -> None:
        """定时任务检查器（每30秒轮询一次）"""
        logger.info("定时检查器启动")
        while True:
            await asyncio.sleep(30 - datetime.now().second % 30)  # 每30秒对齐检查
            now = datetime.now(timezone(timedelta(hours=8)))  # UTC+8 时区
            current_time = now.strftime("%H:%M")
            
            for task in self.tasks.copy():  # 复制列表避免迭代中修改
                if task["time"] == current_time and self._should_trigger(task, now):
                    try:
                        output = await self._execute_script(task["script_name"])
                        await self._send_task_result(task, output)
                        task["last_run"] = now.isoformat()  # 记录最后执行时间
                        self._save_tasks()  # 持久化更新
                    except Exception as e:
                        logger.error(f"任务执行失败: {str(e)}")

    def _should_trigger(self, task: Dict, now: datetime) -> bool:
        """判断任务是否应触发（每日一次）"""
        last_run = datetime.fromisoformat(task["last_run"]) if task.get("last_run") else None
        return not last_run or (now - last_run).total_seconds() >= 86400  # 24小时间隔

    async def _send_task_result(self, task: Dict, message: str) -> None:
        """使用 AstrBot 标准消息发送接口（unified_msg_origin + MessageChain）"""
        try:
            # 构造消息链（纯文本，限制长度2000字符）
            message_chain = MessageChain([Plain(text=message[:2000])])
            
            # 调用 AstrBot 上下文的标准发送方法：target + chain（位置参数）
            await self.context.send_message(
                task["unified_msg_origin"],  # 会话唯一标识作为 target
                message_chain                # 消息链
            )
            logger.debug("消息已成功发送到目标会话")
        except Exception as e:
            logger.error(f"消息发送失败: {str(e)}，任务详情：{task}")

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
            result = subprocess.run(
                ["python", script_path],
                capture_output=True,
                text=True,
                timeout=30,
                encoding="utf-8",
                check=True
            )
            return result.stdout
        except subprocess.TimeoutExpired:
            raise TimeoutError("执行超时（30秒限制）")
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"执行失败（代码{e.returncode}）: {e.stderr}")
        except Exception as e:
            raise RuntimeError(f"未知错误: {str(e)}")

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
                
            output = await self._execute_script(parts[1])
            yield event.plain_result(f"✅ 执行成功\n{output[:1500]}")
            
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
            f"任务ID：{new_task['task_id']}"
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
            status = "✅ 已激活" if task.get("last_run") else "⏳ 待触发"
            last_run = (
                datetime.fromisoformat(task["last_run"]).strftime("%m/%d %H:%M") 
                if task.get("last_run") else "尚未执行"
            )
            
            task_list.extend([
                f"▪️ 任务 {idx}",
                f"名称：{task['script_name']}",
                f"时间：每日 {task['time']}",
                f"状态：{status}",
                f"最后执行：{last_run}",
                f"任务ID：{task['task_id']}",
                "━━━━━━━━━━━━━━━━"
            ])
            
        yield event.plain_result("\n".join(task_list))

    async def _show_help(self, event: AstrMessageEvent) -> MessageEventResult:
        """显示帮助信息"""
        help_msg = """
📘 定时任务插件使用指南

【命令列表】
/定时 添加 [脚本名] [时间] - 创建每日定时任务（脚本需放在 plugin_data/ZaskManager 下）
/定时 删除 [任务ID或名称] - 删除当前会话的任务
/定时 列出 - 查看当前会话的所有任务
/执行 [脚本名] - 立即执行脚本并返回结果

【示例】
/定时 添加 数据备份 08:30   # 每日08:30执行“数据备份.py”
/定时 删除 数据备份_0830_12345  # 通过任务ID精准删除
/定时 列出                # 查看当前会话所有任务
/执行 数据备份            # 立即运行“数据备份.py”

🛑 注意：
- 任务ID由「脚本名+时间+会话ID」生成，添加后可查看
- 定时任务每日执行一次，脚本执行超时30秒会自动终止
- 仅当前会话（群/私聊）的任务会被列出/删除
        """.strip()
        yield event.plain_result(help_msg)

    async def terminate(self) -> None:
        """插件卸载时停止定时检查任务"""
        if hasattr(self, "schedule_checker_task"):
            self.schedule_checker_task.cancel()
