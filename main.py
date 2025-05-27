from pkg.plugin.context import register, handler, BasePlugin, APIHost, EventContext
from pkg.plugin.events import GroupMessageReceived, PersonMessageReceived
from pkg.platform.types import *
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
    """生成任务唯一标识"""
    return f"{task['script_name']}_{task['time'].replace(':', '')}_{task['target_type'][0]}"

@register(name="WaskManager", description="全功能定时任务插件", version="3.2", author="xiaoxin")
class WaskManagerPlugin(BasePlugin):
    def __init__(self, host: APIHost):
        super().__init__(host)
        self.tasks: List[Dict] = []
        self.data_dir = os.path.join(os.path.dirname(__file__), "data")
        self.tasks_file = os.path.join(os.path.dirname(__file__), "tasks.json")
        os.makedirs(self.data_dir, exist_ok=True)
    
    async def initialize(self):
        self.load_tasks()
        await self.restart_scheduler()
        self.ap.logger.info("插件初始化完成")

    # 保留原有基础方法...

    @handler(GroupMessageReceived)
    @handler(PersonMessageReceived)
    async def message_handler(self, ctx: EventContext):
        msg = str(ctx.event.message_chain).strip()
        parts = msg.split(maxsplit=3)
        
        try:
            # 处理 /执行 命令
            if len(parts) >= 2 and parts[0] == "/执行":
                script_name = parts[1]
                output = await self.execute_script(script_name)
                await ctx.reply(MessageChain([Plain(f"✅ 执行成功\n{output[:1500]}")]))
                ctx.prevent_default()

            # 处理 /定时 命令
            elif len(parts) >= 2 and parts[0] == "/定时":
                if parts[1] == "添加" and len(parts) == 4:
                    await self.add_task(ctx, parts[2], parts[3])
                    ctx.prevent_default()
                elif parts[1] == "删除" and len(parts) == 3:
                    await self.delete_task(ctx, parts[2])
                    ctx.prevent_default()
                elif parts[1] == "列出":
                    await self.list_tasks(ctx)
                    ctx.prevent_default()

        except Exception as e:
            await ctx.reply(MessageChain([Plain(f"❌ 错误: {str(e)}")]))
            ctx.prevent_default()

    async def add_task(self, ctx: EventContext, name: str, time_str: str):
        """添加任务（自动绑定当前会话）"""
        if not re.fullmatch(r"^([01]\d|2[0-3]):([0-5]\d)$", time_str):
            raise ValueError("时间格式应为 HH:MM，例如：08:30")

        script_path = os.path.join(self.data_dir, f"{name}.py")
        if not os.path.exists(script_path):
            available = ", ".join(f.replace('.py', '') for f in os.listdir(self.data_dir))
            raise FileNotFoundError(f"脚本不存在！可用脚本: {available or '无'}")

        new_task = {
            "script_name": name,
            "time": time_str,
            "target_type": ctx.event.launcher_type.value,  # 记录来源类型
            "target_id": str(ctx.event.launcher_id),      # 记录来源ID
            "last_run": None,
            "created": datetime.now(china_tz).isoformat(),
            "task_id": ""  # 下面生成
        }
        new_task["task_id"] = generate_task_id(new_task)
        
        # 检查是否已存在相同任务
        if any(t["task_id"] == new_task["task_id"] for t in self.tasks):
            raise ValueError(f"该时段任务已存在（ID: {new_task['task_id']}）")
            
        self.tasks.append(new_task)
        self.save_tasks()
        
        reply_msg = (
            "✅ 定时任务创建成功\n"
            f"名称：{name}\n"
            f"时间：每日 {time_str}\n"
            f"绑定到：{ctx.event.launcher_type.name}\n"
            f"唯一ID：{new_task['task_id']}"
        )
        await ctx.reply(MessageChain([Plain(reply_msg)]))

    async def delete_task(self, ctx: EventContext, identifier: str):
        """删除任务（支持名称或完整ID）"""
        target_type = ctx.event.launcher_type.value
        target_id = str(ctx.event.launcher_id)
        
        # 查找匹配任务
        deleted = []
        for task in self.tasks.copy():
            # 检查会话匹配
            if task["target_type"] != target_type or task["target_id"] != target_id:
                continue
                
            # 匹配ID或名称
            if identifier in (task["task_id"], task["script_name"]):
                deleted.append(task)
                self.tasks.remove(task)
        
        if not deleted:
            available_tasks = "\n".join([f"· {t['task_id']}" for t in self.get_current_tasks(ctx)])
            raise ValueError(f"未找到匹配任务！当前任务：\n{available_tasks or '无'}")
        
        self.save_tasks()
        
        # 生成删除报告
        report = ["✅ 已删除以下任务："]
        for task in deleted:
            report.append(
                f"▫️ {task['script_name']} ({task['time']})\n"
                f"  任务ID: {task['task_id']}"
            )
        await ctx.reply(MessageChain([Plain("\n".join(report))]))

    def get_current_tasks(self, ctx: EventContext) -> List[Dict]:
        """获取当前会话的任务"""
        return [
            t for t in self.tasks 
            if t["target_type"] == ctx.event.launcher_type.value 
            and t["target_id"] == str(ctx.event.launcher_id)
        ]

    async def list_tasks(self, ctx: EventContext):
        """显示当前会话的任务"""
        current_tasks = self.get_current_tasks(ctx)
        
        if not current_tasks:
            await ctx.reply(MessageChain([Plain("当前会话没有定时任务")]))
            return
            
        task_list = [
            "📅 当前定时任务（输入/定时 删除 [ID或名称] 来管理）",
            f"会话类型：{ctx.event.launcher_type.name}",
            "━━━━━━━━━━━━━━"
        ]
        
        for task in current_tasks:
            status = "✅ 已激活" if task.get("last_run") else "⏳ 待触发"
            last_run = datetime.fromisoformat(task["last_run"]).strftime("%m/%d %H:%M") if task["last_run"] else "尚未执行"
            task_list.append(
                f"▪️ 任务名称：{task['script_name']}\n"
                f"  执行时间：每日 {task['time']}\n"
                f"  任务状态：{status}\n"
                f"  最后执行：{last_run}\n"
                f"  唯一ID：{task['task_id']}\n"
                "━━━━━━━━━━━━━━"
            )
            
        await ctx.reply(MessageChain([Plain("\n".join(task_list))]))

    def __del__(self):
        if hasattr(self, "check_task"):
            self.check_task.cancel()
