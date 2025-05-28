from astrbot.api.event import filter, AstrMessageEvent, MessageEventResult
from astrbot.api.star import Context, Star, register
from astrbot.api import logger
from astrbot.api.message_components import Plain, Image
import os
import re
import json
import asyncio
import subprocess
from datetime import datetime, timedelta, timezone
from typing import List, Dict

# 创建UTC+8时区
china_tz = timezone(timedelta(hours=8))

def generate_task_id(task: Dict) -> str:
    """生成唯一任务标识"""
    return f"{task['script_name']}_{task['time'].replace(':', '')}_{task['target_type'][0]}_{task['target_id']}"

@register("ZaskManager", "xiaoxin", "全功能定时任务插件", "3.5", "https://github.com/styy88/ZaskManager")
class ZaskManager(Star):
    def __init__(self, context: Context, config: dict):  # ✅ 添加 config 参数
        super().__init__(context)
        self.config = config  # ✅ 加载配置
        
        # 标准化路径配置
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
                    ]
                logger.info(f"成功加载 {len(self.tasks)} 个定时任务")
        except Exception as e:
            logger.error(f"任务加载失败: {str(e)}")
            self.tasks = []

    def _save_tasks(self):
        """安全保存任务数据"""
        with open(self.tasks_file, "w", encoding="utf-8") as f:
            json.dump(self.tasks, f, indent=2, ensure_ascii=False)

    async def schedule_checker(self):
        """定时任务检查器"""
        logger.info("定时检查器启动")
        while True:
            await asyncio.sleep(30 - datetime.now().second % 30)
            now = datetime.now(china_tz)
            current_time = now.strftime("%H:%M")
            
            for task in self.tasks.copy():
                if task["time"] == current_time and self._should_trigger(task, now):
                    try:
                        output = await self._execute_script(task["script_name"])
                        await self._send_task_result(task, output)
                        task["last_run"] = now.isoformat()
                        self._save_tasks()
                    except Exception as e:
                        logger.error(f"任务执行失败: {str(e)}")

    def _should_trigger(self, task: Dict, now: datetime) -> bool:
        """判断是否应该触发任务"""
        last_run = datetime.fromisoformat(task["last_run"]) if task.get("last_run") else None
        return not last_run or (now - last_run).total_seconds() >= 86400

    async def _send_task_result(self, task: Dict, message: str):
        """发送任务结果"""
        try:
            chain = [Plain(message[:2000])]
            if task["target_type"] == "group":
                await self.context.send_message(
                    unified_msg_origin=f"group_{task['target_id']}",
                    chain=chain
                )
            else:
                await self.context.send_message(
                    unified_msg_origin=f"private_{task['target_id']}",
                    chain=chain
                )
        except Exception as e:
            logger.error(f"消息发送失败: {str(e)}")

    async def _execute_script(self, script_name: str) -> str:
        """执行脚本文件（修复路径）"""
        script_path = os.path.join(self.plugin_root, f"{script_name}.py")  # ✅ 使用正确的根目录
        
        if not os.path.exists(script_path):
            available = ", ".join(
                f.replace('.py', '') 
                for f in os.listdir(self.plugin_root)  # ✅ 使用 plugin_root
                if f.endswith('.py')
            )
            raise FileNotFoundError(f"脚本不存在！可用脚本: {available or '无'}")

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
    async def schedule_command(self, event: AstrMessageEvent):
        """处理定时命令"""
        try:
            parts = event.message_str.split(maxsplit=3)
            if len(parts) < 2:
                raise ValueError("命令格式错误，请输入'/定时 帮助'查看用法")

            if parts[1] == "添加":
                if len(parts) != 4:
                    raise ValueError("格式应为：/定时 添加 [脚本名] [时间]")
                await self._add_task(event, parts[2], parts[3])
                
            elif parts[1] == "删除":
                if len(parts) != 3:
                    raise ValueError("格式应为：/定时 删除 [任务ID或名称]")
                await self._delete_task(event, parts[2])
                
            elif parts[1] == "列出":
                await self._list_tasks(event)
                
            else:
                await self._show_help(event)

        except Exception as e:
            yield event.plain_result(f"❌ 错误: {str(e)}")

    @filter.command("执行")
    async def execute_command(self, event: AstrMessageEvent):
        """处理立即执行命令"""
        try:
            parts = event.message_str.split(maxsplit=1)
            if len(parts) < 2:
                raise ValueError("格式应为：/执行 [脚本名]")
                
            output = await self._execute_script(parts[1])
            yield event.plain_result(f"✅ 执行成功\n{output[:1500]}")
            
        except Exception as e:
            yield event.plain_result(f"❌ 错误: {str(e)}")

    async def _add_task(self, event: AstrMessageEvent, name: str, time_str: str):
        """添加定时任务"""
        if not name or not time_str:
            raise ValueError("参数不能为空，格式：/定时 添加 [脚本名] [时间]")
        
        if not re.fullmatch(r"^([01]\d|2[0-3]):([0-5]\d)$", time_str):
            raise ValueError("时间格式应为 HH:MM（24小时制），例如：14:00")

        # 获取会话信息
        target_type = "group" if event.group_id else "private"
        target_id = event.group_id or event.get_sender_id()

        # 脚本存在性检查
        script_path = os.path.join(self.data_dir, f"{name}.py")
        if not os.path.exists(script_path):
            available = ", ".join(f.replace('.py', '') for f in os.listdir(self.data_dir))
            raise FileNotFoundError(f"脚本不存在！可用脚本: {available or '无'}")

        # 构建任务对象
        new_task = {
            "script_name": name,
            "time": time_str,
            "target_type": target_type,
            "target_id": target_id,
            "last_run": None,
            "created": datetime.now(china_tz).isoformat()
        }
        new_task["task_id"] = generate_task_id(new_task)
        
        # 冲突检测
        if any(t["task_id"] == new_task["task_id"] for t in self.tasks):
            raise ValueError(f"该时段任务已存在（ID: {new_task['task_id']}）")
            
        self.tasks.append(new_task)
        self._save_tasks()
    
        reply_msg = (
            "✅ 定时任务创建成功\n"
            f"名称：{name}\n"
            f"时间：每日 {time_str}\n"
            f"绑定到：{'群聊' if target_type == 'group' else '私聊'}\n"
            f"任务ID：{new_task['task_id']}"
        )
        yield event.plain_result(reply_msg)

    async def _delete_task(self, event: AstrMessageEvent, identifier: str):
        """删除当前会话的任务"""
        target_type = "group" if event.group_id else "private"
        target_id = event.group_id or event.get_sender_id()
        
        current_tasks = [
            t for t in self.tasks 
            if t["target_type"] == target_type
            and t["target_id"] == target_id
        ]
        
        if not current_tasks:
            yield event.plain_result("当前会话没有定时任务")
            return
            
        deleted = []
        for task in current_tasks.copy():
            if identifier in (task["task_id"], task["script_name"]):
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
                f"  任务ID：{task['task_id']}\n"
                "━━━━━━━━━━━━━━━━"
            )
        yield event.plain_result("\n".join(report))

    async def _list_tasks(self, event: AstrMessageEvent):
        """列出当前会话任务"""
        target_type = "group" if event.group_id else "private"
        target_id = event.group_id or event.get_sender_id()
        
        current_tasks = [
            t for t in self.tasks 
            if t["target_type"] == target_type
            and t["target_id"] == target_id
        ]
        
        if not current_tasks:
            yield event.plain_result("当前会话没有定时任务")
            return
            
        task_list = [
            "📅 当前会话定时任务列表",
            f"会话类型：{'群聊' if target_type == 'group' else '私聊'}",
            f"会话ID：{target_id}",
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

    async def _show_help(self, event: AstrMessageEvent):
        """显示帮助信息"""
        help_msg = """
📘 定时任务插件使用指南

【命令列表】
/定时 添加 [脚本名] [时间] - 创建新任务
/定时 删除 [任务ID或名称] - 删除任务
/定时 列出 - 显示当前会话任务
/执行 [脚本名] - 立即执行脚本

【示例】
/定时 添加 数据备份 08:30
/定时 删除 数据备份_0830_g_12345
/定时 列出
/执行 数据备份

🛑 注意：任务ID可在添加成功时获得
        """.strip()
        yield event.plain_result(help_msg)

    async def terminate(self):
        """插件卸载时停止所有任务"""
        if hasattr(self, "schedule_checker_task"):
            self.schedule_checker_task.cancel()
