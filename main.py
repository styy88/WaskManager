# main.py
from pkg.plugin.context import register, handler, BasePlugin, APIHost, EventContext
from pkg.plugin.events import GroupMessageReceived, PersonMessageReceived
from pkg.platform.types import *
import os
import re
import json
import asyncio
import subprocess
import urllib.parse
from datetime import datetime, timedelta, timezone

# 创建UTC+8时区
china_tz = timezone(timedelta(hours=8))

@register(name="TaskManager", description="全功能定时任务插件", version="2.2", author="xiaoxin")
class TaskManagerPlugin(BasePlugin):
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
        if os.path.exists(self.tasks_file):
            try:
                with open(self.tasks_file, "r", encoding="utf-8") as f:
                    self.tasks = json.load(f)
                self.ap.logger.info(f"成功加载 {len(self.tasks)} 个定时任务")
            except Exception as e:
                self.ap.logger.error(f"任务加载失败: {str(e)}")

    def save_tasks(self):
        with open(self.tasks_file, "w", encoding="utf-8") as f:
            json.dump(self.tasks, f, indent=2, ensure_ascii=False)

    async def schedule_checker(self):
        self.ap.logger.info("定时检查器启动")
        while True:
            await asyncio.sleep(30 - datetime.now().second % 30)
            now = datetime.now(china_tz)
            current_time = now.strftime("%H:%M")
            
            for task in self.tasks.copy():
                if task["time"] == current_time and self.should_trigger(task, now):
                    try:
                        # 执行脚本
                        output = await self.execute_script(task["script_name"])
                        
                        # 发送消息到原会话
                        await self.send_task_result(
                            target_type=task["target_type"],
                            target_id=task["target_id"],
                            message=output
                        )
                        
                        # 更新执行时间
                        task["last_run"] = now.isoformat()
                        self.save_tasks()
                        
                    except Exception as e:
                        self.ap.logger.error(f"任务执行失败: {str(e)}")

    async def send_task_result(self, target_type, target_id, message):
        """安全发送消息到指定目标"""
        try:
            await self.host.send_active_message(
                adapter=self.host.get_platform_adapters()[0],
                target_type=target_type,
                target_id=str(target_id),
                message=MessageChain([Plain(message[:2000])])  # 限制消息长度
            )
        except Exception as e:
            self.ap.logger.error(f"消息发送失败: {str(e)}")

    def should_trigger(self, task, now):
        last_run = datetime.fromisoformat(task["last_run"]) if task.get("last_run") else None
        return not last_run or (now - last_run).total_seconds() >= 86400

    async def execute_script(self, script_name: str):
        """支持中文文件名的脚本执行"""
        # 将中文转换为安全文件名
        safe_name = urllib.parse.quote(script_name, safe='')
        script_path = os.path.join(self.data_dir, f"{safe_name}.py")
        
        if not os.path.exists(script_path):
            raise FileNotFoundError(f"脚本文件不存在: {script_name}.py")

        result = subprocess.run(
            ["python", script_path],
            capture_output=True,
            text=True,
            timeout=30,
            encoding="utf-8"
        )
        
        if result.returncode != 0:
            raise RuntimeError(f"脚本执行失败: {result.stderr}")
        
        return result.stdout

    @handler(GroupMessageReceived)
    @handler(PersonMessageReceived)
    async def message_handler(self, ctx: EventContext):
        msg = str(ctx.event.message_chain).strip()
        parts = msg.split(maxsplit=3)
        
        try:
            if parts[0] == "/执行" and len(parts) >= 2:
                script_name = " ".join(parts[1:])
                output = await self.execute_script(script_name)
                await ctx.reply(MessageChain([Plain(output[:2000])]))
                ctx.prevent_default()

            elif parts[0] == "/定时":
                if len(parts) >= 4 and parts[1] == "添加":
                    script_name = " ".join(parts[2:-1])
                    time_str = parts[-1]
                    await self.add_task(ctx, script_name, time_str)
                    ctx.prevent_default()
                    
                elif len(parts) >= 3 and parts[1] == "删除":
                    script_name = " ".join(parts[2:])
                    await self.delete_task(script_name)
                    ctx.prevent_default()
                    
                elif parts[1] == "列出":
                    await self.list_tasks(ctx)
                    ctx.prevent_default()

        except Exception as e:
            await ctx.reply(MessageChain([Plain(f"❌ 错误: {str(e)}")]))
            ctx.prevent_default()

    async def add_task(self, ctx: EventContext, script_name: str, time_str: str):
        """支持中文任务名的添加逻辑"""
        if not re.match(r"^([01]\d|2[0-3]):([0-5]\d)$", time_str):
            raise ValueError("时间格式应为 HH:MM")

        # 检查脚本是否存在
        safe_name = urllib.parse.quote(script_name, safe='')
        if not os.path.exists(os.path.join(self.data_dir, f"{safe_name}.py")):
            raise FileNotFoundError(f"脚本 {script_name}.py 不存在")

        self.tasks.append({
            "script_name": script_name,
            "time": time_str,
            "target_type": ctx.event.launcher_type,
            "target_id": ctx.event.launcher_id,
            "last_run": None,
            "created": datetime.now(china_tz).isoformat()
        })
        self.save_tasks()
        await ctx.reply(MessageChain([Plain(f"✅ 定时任务创建成功\n名称: {script_name}\n时间: 每日 {time_str}")]))

    async def delete_task(self, script_name: str):
        original_count = len(self.tasks)
        self.tasks = [t for t in self.tasks if t["script_name"] != script_name]
        if len(self.tasks) == original_count:
            raise ValueError("未找到指定任务")
        self.save_tasks()

    async def list_tasks(self, ctx: EventContext):
        task_list = ["🕒 当前定时任务列表 🕒"]
        for task in self.tasks:
            status = "✅ 已激活" if task.get("last_run") else "⏳ 待触发"
            task_list.append(
                f"名称: {task['script_name']}\n"
                f"时间: 每日 {task['time']}\n"
                f"状态: {status}\n"
                "━━━━━━━━━━━━━━"
            )
        await ctx.reply(MessageChain([Plain("\n".join(task_list))]))

    def __del__(self):
        if hasattr(self, "check_task"):
            self.check_task.cancel()
