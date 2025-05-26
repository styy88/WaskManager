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

# 创建UTC+8时区
china_tz = timezone(timedelta(hours=8))

@register(name="TaskManager", description="定时任务管理插件", version="1.0", author="xiaoxin")
class TaskManagerPlugin(BasePlugin):
    def __init__(self, host: APIHost):
        super().__init__(host)
        self.tasks = []
        self.data_dir = os.path.join(os.path.dirname(__file__), "data")
        self.tasks_file = os.path.join(os.path.dirname(__file__), "tasks.json")
        os.makedirs(self.data_dir, exist_ok=True)
        self.load_tasks()
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
            await asyncio.sleep(30)  # 每30秒检查一次
            now = datetime.now(china_tz)
            current_time = now.strftime("%H:%M")
            
            for task in self.tasks:
                if task["time"] == current_time:
                    if self.should_trigger(task, now):
                        await self.execute_script(task["name"])
                        task["last_run"] = now.isoformat()
                        self.save_tasks()

    def should_trigger(self, task, now):
        """判断是否应该触发"""
        last_run = datetime.fromisoformat(task["last_run"]) if task.get("last_run") else None
        return not last_run or (now - last_run).total_seconds() > 86400  # 24小时触发一次

    async def execute_script(self, script_name):
        """执行脚本"""
        script_path = os.path.join(self.data_dir, f"{script_name}.py")
        if not os.path.exists(script_path):
            return

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

    @handler(GroupMessageReceived)
    @handler(PersonMessageReceived)
    async def message_handler(self, ctx: EventContext):
        """消息处理"""
        msg = str(ctx.event.message_chain).strip()
        parts = msg.split(maxsplit=2)
    
    is_processed = False  # 命令处理标志
    
    # 命令路由
    if msg.startswith("/剩余时间"):
        await self.run_remaining_time(ctx)
        is_processed = True
        
    elif msg.startswith("/添加") and len(parts) == 3:
        script_name, schedule_time = parts[1], parts[2]
        await self.add_task(ctx, script_name, schedule_time)
        is_processed = True
        
    elif msg.startswith("/删除") and len(parts) == 2:
        script_name = parts[1]
        await self.delete_task(ctx, script_name)
        is_processed = True
        
    elif msg == "/任务列表":
        await self.list_tasks(ctx)
        is_processed = True

    # 拦截处理
    if is_processed:
        # 阻止默认回复和后续处理
        ctx.prevent_default()
        ctx.prevent_postprocess()
        
        # 调试日志（可选）
        self.ap.logger.info(f"Handled command: {msg}", 
                          sender=ctx.event.sender_id,
                          launcher=ctx.event.launcher_id)

    async def run_remaining_time(self, ctx: EventContext):
        """执行剩余时间脚本"""
        output = await self.execute_script("remaining_time")
        if output:
            await ctx.reply(MessageChain([Plain(output)]))
        else:
            await ctx.reply(MessageChain([Plain("执行失败，请检查脚本")]))

    async def add_task(self, ctx: EventContext, name: str, time_str: str):
        """添加定时任务"""
        # 验证脚本名称
        if not re.match(r"^[a-zA-Z0-9_]+$", name):
            await ctx.reply(MessageChain([Plain("无效的脚本名称，只允许字母数字和下划线")]))
            return

        # 验证时间格式
        if not re.match(r"^([0-1][0-9]|2[0-3]):[0-5][0-9]$", time_str):
            await ctx.reply(MessageChain([Plain("时间格式应为HH:MM，例如08:30")]))
            return

        # 检查脚本存在
        script_path = os.path.join(self.data_dir, f"{name}.py")
        if not os.path.exists(script_path):
            await ctx.reply(MessageChain([Plain(f"脚本{name}.py不存在")]))
            return

        # 防止重复添加
        if any(t["name"] == name for t in self.tasks):
            await ctx.reply(MessageChain([Plain("该任务已存在")]))
            return

        new_task = {
            "name": name,
            "time": time_str,
            "last_run": None,
            "created": datetime.now(china_tz).isoformat()
        }
        self.tasks.append(new_task)
        self.save_tasks()
        await ctx.reply(MessageChain([Plain(f"已添加定时任务: {name} 每天{time_str}执行")]))

    async def delete_task(self, ctx: EventContext, name: str):
        """删除定时任务"""
        original_count = len(self.tasks)
        self.tasks = [t for t in self.tasks if t["name"] != name]
        
        if len(self.tasks) < original_count:
            self.save_tasks()
            await ctx.reply(MessageChain([Plain(f"已删除任务: {name}")]))
        else:
            await ctx.reply(MessageChain([Plain("未找到该任务")]))

    async def list_tasks(self, ctx: EventContext):
        """列出所有任务"""
        if not self.tasks:
            await ctx.reply(MessageChain([Plain("当前没有定时任务")]))
            return

        task_list = []
        for task in self.tasks:
            status = "已激活" if task.get("last_run") else "未运行"
            task_list.append(
                f"任务名称: {task['name']}\n"
                f"执行时间: {task['time']}\n"
                f"最后执行: {task.get('last_run', '从未执行')}\n"
                f"创建时间: {task['created'][:10]}"
            )

        await ctx.reply(MessageChain([Plain("\n\n".join(task_list))]))

    def __del__(self):
        if hasattr(self, "check_task"):
            self.check_task.cancel()
