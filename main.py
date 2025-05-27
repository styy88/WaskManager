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
    """生成唯一任务标识"""
    return f"{task['script_name']}_{task['time'].replace(':', '')}_{task['target_type'][0]}_{task['target_id']}"

@register(name="WaskManagerPro", description="增强版定时任务插件", version="3.4", author="xiaoxin")
class WaskManagerPlugin(BasePlugin):
    def __init__(self, host: APIHost):
        super().__init__(host)
        self.tasks: List[Dict] = []
        self.data_dir = os.path.join(os.path.dirname(__file__), "data")
        self.tasks_file = os.path.join(os.path.dirname(__file__), "tasks.json")
        os.makedirs(self.data_dir, exist_ok=True)
    
    async def initialize(self):
        """修复初始化方法"""
        self._load_tasks()
        await self.restart_scheduler()
        self.ap.logger.info("插件初始化完成")

    async def restart_scheduler(self):
        """重启任务调度器"""
        if hasattr(self, "check_task"):
            self.check_task.cancel()
        self.check_task = asyncio.create_task(self.schedule_checker())

    def _load_tasks(self):
        """安全加载任务数据"""
        try:
            if os.path.exists(self.tasks_file):
                with open(self.tasks_file, "r", encoding="utf-8") as f:
                    raw_tasks = json.load(f)
                    # 兼容旧数据格式
                    self.tasks = [
                        {**task, "task_id": task.get("task_id") or generate_task_id(task)}
                        for task in raw_tasks
                    ]
                self.ap.logger.info(f"成功加载 {len(self.tasks)} 个定时任务")
        except Exception as e:
            self.ap.logger.error(f"任务加载失败: {str(e)}")
            self.tasks = []

    def _save_tasks(self):
        """安全保存任务数据"""
        with open(self.tasks_file, "w", encoding="utf-8") as f:
            json.dump(self.tasks, f, indent=2, ensure_ascii=False)

    async def schedule_checker(self):
        """定时任务检查器"""
        self.ap.logger.info("定时检查器启动")
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
                        self.ap.logger.error(f"任务执行失败: {str(e)}")

    def _should_trigger(self, task: Dict, now: datetime) -> bool:
        """判断是否应该触发任务"""
        last_run = datetime.fromisoformat(task["last_run"]) if task.get("last_run") else None
        return not last_run or (now - last_run).total_seconds() >= 86400

    async def _send_task_result(self, task: Dict, message: str):
        """发送任务结果"""
        try:
            await self.host.send_active_message(
                adapter=self.host.get_platform_adapters()[0],
                target_type=task["target_type"],
                target_id=task["target_id"],
                message=MessageChain([Plain(message[:2000])])
            )
        except Exception as e:
            self.ap.logger.error(f"消息发送失败: {str(e)}")

    async def _execute_script(self, script_name: str) -> str:
        """执行脚本文件"""
        script_path = os.path.join(self.data_dir, f"{script_name}.py")
        
        if not os.path.exists(script_path):
            available = ", ".join(f.replace('.py', '') for f in os.listdir(self.data_dir))
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

    @handler(GroupMessageReceived)
    @handler(PersonMessageReceived)
    async def message_handler(self, ctx: EventContext):
        """消息处理器"""
        try:
            # 兼容不同框架版本的消息链获取
            query = getattr(ctx.event, 'query', ctx.event)
            msg = str(query.message_chain).strip()
            
            if not (msg.startswith('/定时') or msg.startswith('/执行')):
                return
                
            parts = msg.split(maxsplit=3)
            
            if parts[0] == "/定时":
                await self._handle_schedule_command(ctx, parts)
            elif parts[0] == "/执行":
                await self._handle_execute_command(ctx, parts)
                
            ctx.prevent_default()

        except Exception as e:
            await ctx.reply(MessageChain([Plain(f"❌ 错误: {str(e)}")]))
            ctx.prevent_default()

    async def _handle_schedule_command(self, ctx: EventContext, parts: List[str]):
        """处理定时命令"""
        if len(parts) < 2:
            raise ValueError("命令格式错误，请输入'/定时 帮助'查看用法")
            
        if parts[1] == "添加":
            if len(parts) != 4:
                raise ValueError("格式应为：/定时 添加 [脚本名] [时间]")
            await self._add_task(ctx, parts[2], parts[3])
        elif parts[1] == "删除":
            if len(parts) != 3:
                raise ValueError("格式应为：/定时 删除 [任务ID或名称]")
            await self._delete_task(ctx, parts[2])
        elif parts[1] == "列出":
            await self._list_tasks(ctx)
        else:
            await self._show_help(ctx)

    async def _handle_execute_command(self, ctx: EventContext, parts: List[str]):
        """处理立即执行命令"""
        if len(parts) < 2:
            raise ValueError("格式应为：/执行 [脚本名]")
            
        output = await self._execute_script(parts[1])
        await ctx.reply(MessageChain([Plain(f"✅ 执行成功\n{output[:1500]}")]))

    async def _add_task(self, ctx: EventContext, name: str, time_str: str):
        """添加新任务"""
        if not re.fullmatch(r"^([01]\d|2[0-3]):([0-5]\d)$", time_str):
            raise ValueError("时间格式应为 HH:MM（24小时制），例如：14:00")

        script_path = os.path.join(self.data_dir, f"{name}.py")
        if not os.path.exists(script_path):
            available = ", ".join(f.replace('.py', '') for f in os.listdir(self.data_dir))
            raise FileNotFoundError(f"脚本不存在！可用脚本: {available or '无'}")

        new_task = {
            "script_name": name,
            "time": time_str,
            "target_type": ctx.event.launcher_type.value,
            "target_id": str(ctx.event.launcher_id),
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
            f"绑定到：{ctx.event.launcher_type.name}\n"
            f"任务ID：{new_task['task_id']}"
        )
        await ctx.reply(MessageChain([Plain(reply_msg)]))

    async def _delete_task(self, ctx: EventContext, identifier: str):
        """删除当前会话的任务"""
        current_tasks = [
            t for t in self.tasks 
            if t["target_type"] == ctx.event.launcher_type.value 
            and t["target_id"] == str(ctx.event.launcher_id)
        ]
        
        if not current_tasks:
            await ctx.reply(MessageChain([Plain("当前会话没有定时任务")]))
            return
            
        # 匹配任务
        deleted = []
        for task in current_tasks.copy():
            if identifier in (task["task_id"], task["script_name"]):
                self.tasks.remove(task)
                deleted.append(task)
                
        if not deleted:
            available_ids = "\n".join([f"· {t['task_id']}" for t in current_tasks])
            raise ValueError(f"未找到匹配任务，当前可用ID：\n{available_ids}")
            
        self._save_tasks()
        
        # 生成报告
        report = ["✅ 已删除以下任务："]
        for task in deleted:
            report.append(
                f"▫️ {task['script_name']} ({task['time']})\n"
                f"  任务ID：{task['task_id']}\n"
                "━━━━━━━━━━━━━━━━"
            )
        await ctx.reply(MessageChain([Plain("\n".join(report))]))

    async def _list_tasks(self, ctx: EventContext):
        """列出当前会话任务"""
        current_tasks = [
            t for t in self.tasks 
            if t["target_type"] == ctx.event.launcher_type.value 
            and t["target_id"] == str(ctx.event.launcher_id)
        ]
        
        if not current_tasks:
            await ctx.reply(MessageChain([Plain("当前会话没有定时任务")]))
            return
            
        task_list = [
            "📅 当前会话定时任务列表",
            f"会话类型：{ctx.event.launcher_type.name}",
            f"会话ID：{ctx.event.launcher_id}",
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
            
        await ctx.reply(MessageChain([Plain("\n".join(task_list))]))

    async def _show_help(self, ctx: EventContext):
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
        await ctx.reply(MessageChain([Plain(help_msg)]))

    def __del__(self):
        """清理资源"""
        if hasattr(self, "check_task"):
            self.check_task.cancel()
