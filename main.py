# main.py
from pkg.plugin.context import register, handler, BasePlugin, APIHost, EventContext
from pkg.plugin.events import GroupMessageReceived, PersonMessageReceived
from pkg.platform.types import *
import os
import re
import json
import asyncio
import subprocess
from datetime import datetime, timedelta, timezone

china_tz = timezone(timedelta(hours=8))

@register(name="WaskManager", description="定时任务管理插件", version="2.0", author="xiaoxin")
class WaskManagerPlugin(BasePlugin):
    def __init__(self, host: APIHost):
        super().__init__(host)
        self.tasks = []
        self.data_dir = os.path.join(os.path.dirname(__file__), "data")
        self.tasks_file = os.path.join(os.path.dirname(__file__), "tasks.json")
        os.makedirs(self.data_dir, exist_ok=True)
    
    async def initialize(self):
        self.load_tasks()
        await self.restart_scheduler()
        self.ap.logger.info("插件初始化完成")

    async def restart_scheduler(self):
        if hasattr(self, "check_task"):
            self.check_task.cancel()
        self.check_task = asyncio.create_task(self.schedule_checker())
        
    def load_tasks(self):
        """加载定时任务"""
        if os.path.exists(self.tasks_file):
            try:
                with open(self.tasks_file, "r", encoding="utf-8") as f:
                    self.tasks = json.load(f)
            except Exception as e:
                self.ap.logger.error(f"加载任务失败: {str(e)}")

    def save_tasks(self):
        """保存定时任务"""
        try:
            with open(self.tasks_file, "w", encoding="utf-8") as f:
                json.dump(self.tasks, f, indent=2, ensure_ascii=False)
        except Exception as e:
            self.ap.logger.error(f"保存任务失败: {str(e)}")

    async def schedule_checker(self):
        """定时任务检查循环"""
        while True:
            await asyncio.sleep(30)
            now = datetime.now(china_tz)
            current_time = now.strftime("%H:%M")
            
            for task in self.tasks:
                if task["time"] == current_time and self.should_trigger(task, now):
                    await self.execute_script(task["name"])
                    task["last_run"] = now.isoformat()
                    self.save_tasks()

    def should_trigger(self, task, now):
        """判断是否应该触发"""
        last_run = datetime.fromisoformat(task["last_run"]) if task.get("last_run") else None
        return not last_run or (now - last_run).total_seconds() > 86400

    async def execute_script(self, script_name: str):
        """执行指定脚本"""
        script_path = os.path.join(self.data_dir, f"{script_name}.py")
        if not os.path.exists(script_path):
            raise FileNotFoundError(f"脚本 {script_name}.py 不存在")

        try:
            result = subprocess.run(
                ["python", script_path],
                capture_output=True,
                text=True,
                timeout=30
            )
            return result.stdout
        except Exception as e:
            self.ap.logger.error(f"执行脚本失败: {str(e)}")
            return None

    # 消息处理器
    @handler(GroupMessageReceived)
    @handler(PersonMessageReceived)
    async def message_handler(self, ctx: EventContext):
        """统一消息处理"""
        msg = str(ctx.event.message_chain).strip()
        parts = msg.split()
        is_processed = False

        try:
            # 执行即时命令
            if len(parts) >= 2 and parts[0] == "/执行":
                script_name = parts[1]
                output = await self.execute_script(script_name)
                reply = output if output else f"已执行脚本: {script_name}"
                await ctx.reply(MessageChain([Plain(reply)]))
                is_processed = True

            # 定时任务管理
            elif len(parts) >= 2 and parts[0] == "/定时":
                if parts[1] == "添加" and len(parts) == 4:
                    await self.add_task(ctx, parts[2], parts[3])
                    is_processed = True
                elif parts[1] == "删除" and len(parts) == 3:
                    await self.delete_task(ctx, parts[2])
                    is_processed = True
                elif parts[1] == "列出" and len(parts) == 2:
                    await self.list_tasks(ctx)
                    is_processed = True

            if is_processed:
                ctx.prevent_default()
                self.ap.logger.info(f"处理命令: {msg} [用户:{ctx.event.sender_id}]")

        except Exception as e:
            self.ap.logger.error(f"命令处理失败: {str(e)}")
            await ctx.reply(MessageChain([Plain(f"命令执行失败: {str(e)}")]))
            ctx.prevent_default()

    # 定时任务管理功能
    async def add_task(self, ctx: EventContext, name: str, time_str: str):
        """添加定时任务"""
        # 参数验证
        if not re.match(r"^[\w-]+$", name):
            await ctx.reply(MessageChain([Plain("任务名称只能包含字母、数字和下划线")]))
            return

        if not re.match(r"^([01]\d|2[0-3]):([0-5]\d)$", time_str):
            await ctx.reply(MessageChain([Plain("时间格式应为HH:MM，例如08:30")]))
            return

        if any(t["name"] == name for t in self.tasks):
            await ctx.reply(MessageChain([Plain("该任务名称已存在")]))
            return

        script_path = os.path.join(self.data_dir, f"{name}.py")
        if not os.path.exists(script_path):
            await ctx.reply(MessageChain([Plain(f"脚本 {name}.py 不存在，请先创建")]))
            return

        # 添加任务
        self.tasks.append({
            "name": name,
            "time": time_str,
            "last_run": None,
            "created": datetime.now(china_tz).isoformat()
        })
        self.save_tasks()
        await ctx.reply(MessageChain([Plain(f"✅ 已创建定时任务\n名称: {name}\n时间: 每天 {time_str}")]))

    async def delete_task(self, ctx: EventContext, name: str):
        """删除定时任务"""
        original_count = len(self.tasks)
        self.tasks = [t for t in self.tasks if t["name"] != name]
        
        if len(self.tasks) < original_count:
            self.save_tasks()
            await ctx.reply(MessageChain([Plain(f"✅ 已删除任务: {name}")]))
        else:
            await ctx.reply(MessageChain([Plain("❌ 未找到指定任务")]))

    async def list_tasks(self, ctx: EventContext):
        """列出所有定时任务"""
        if not self.tasks:
            await ctx.reply(MessageChain([Plain("当前没有定时任务")]))
            return

        task_list = ["📅 定时任务列表："]
        for idx, task in enumerate(self.tasks, 1):
            last_run = datetime.fromisoformat(task["last_run"]).strftime("%m-%d %H:%M") if task["last_run"] else "从未执行"
            task_list.append(
                f"{idx}. {task['name']}\n"
                f"   ▪ 每日执行时间: {task['time']}\n"
                f"   ▪ 上次执行: {last_run}\n"
                f"   ▪ 创建于: {task['created'][:10]}"
            )

        await ctx.reply(MessageChain([Plain("\n\n".join(task_list))]))

    def __del__(self):
        if hasattr(self, "check_task"):
            self.check_task.cancel()
