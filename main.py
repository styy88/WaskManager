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

@register(name="TaskManager", description="增强版定时任务管理插件", version="2.1", author="xiaoxin")
class TaskManagerPlugin(BasePlugin):
    def __init__(self, host: APIHost):
        super().__init__(host)
        self.tasks = []
        self.data_dir = os.path.join(os.path.dirname(__file__), "data")
        self.tasks_file = os.path.join(os.path.dirname(__file__), "tasks.json")
        os.makedirs(self.data_dir, exist_ok=True)
    
    async def initialize(self):
        """增强初始化方法"""
        self.load_tasks()
        await self.restart_scheduler()
        self.ap.logger.info("定时任务插件已初始化完成")

    async def restart_scheduler(self):
        """重启定时任务检查器"""
        if hasattr(self, "check_task"):
            self.check_task.cancel()
            try:
                await self.check_task
            except asyncio.CancelledError:
                pass
        self.check_task = asyncio.create_task(self.schedule_checker())

    def load_tasks(self):
        """增强任务加载方法"""
        if os.path.exists(self.tasks_file):
            try:
                with open(self.tasks_file, "r", encoding="utf-8") as f:
                    raw_tasks = json.load(f)
                
                now = datetime.now(china_tz)
                valid_tasks = []
                
                for task in raw_tasks:
                    # 校验必填字段
                    required_fields = ["name", "time", "created"]
                    if not all(key in task for key in required_fields):
                        self.ap.logger.warning("发现不完整任务条目，已跳过")
                        continue

                    # 修复时间字段
                    try:
                        # 处理创建时间
                        created = datetime.fromisoformat(task["created"]).astimezone(china_tz)
                        if created > now:
                            task["created"] = now.isoformat()
                            self.ap.logger.warning(f"任务 {task['name']} 创建时间已修正")
                    except Exception:
                        task["created"] = now.isoformat()

                    # 处理最后执行时间
                    if task.get("last_run"):
                        try:
                            last_run = datetime.fromisoformat(task["last_run"]).astimezone(china_tz)
                            if last_run > now:
                                task["last_run"] = None
                                self.ap.logger.warning(f"任务 {task['name']} 最后执行时间已重置")
                        except Exception:
                            task["last_run"] = None

                    valid_tasks.append(task)
                
                self.tasks = valid_tasks
                self.save_tasks()
                self.ap.logger.info(f"已加载 {len(self.tasks)} 个有效任务")
                
            except Exception as e:
                self.ap.logger.error(f"任务加载失败: {str(e)}")
                self.tasks = []

    def save_tasks(self):
        """保存任务时进行数据校验"""
        valid_tasks = []
        for task in self.tasks:
            if not all(key in task for key in ["name", "time", "created"]):
                continue
            if not re.match(r"^([01]\d|2[0-3]):([0-5]\d)$", task["time"]):
                continue
            valid_tasks.append(task)
        
        try:
            with open(self.tasks_file, "w", encoding="utf-8") as f:
                json.dump(valid_tasks, f, indent=2, ensure_ascii=False)
        except Exception as e:
            self.ap.logger.error(f"保存任务失败: {str(e)}")

    async def schedule_checker(self):
        """增强版定时检查循环"""
        self.ap.logger.info("定时任务检查器已启动")
        while True:
            try:
                # 对齐到整秒减少误差
                await asyncio.sleep(30 - datetime.now().second % 30)
                now = datetime.now(china_tz)
                current_time = now.strftime("%H:%M")
                
                self.ap.logger.debug(f"定时检查触发 [系统时间: {now.isoformat()}]")

                # 使用任务副本遍历
                for task in self.tasks.copy():
                    # 精确时间匹配
                    if task["time"] != current_time:
                        continue
                    
                    self.ap.logger.debug(f"检测到待处理任务: {task['name']}")

                    # 执行条件判断
                    if self.should_trigger(task, now):
                        try:
                            self.ap.logger.info(f"开始执行任务: {task['name']}")
                            output = await self.execute_script(task["name"])
                            
                            # 更新执行时间
                            task["last_run"] = now.isoformat()
                            self.save_tasks()
                            
                            log_msg = f"任务 {task['name']} 执行完成"
                            if output:
                                log_msg += f"\n输出: {output[:200]}..." if len(output) > 200 else f"\n输出: {output}"
                            self.ap.logger.info(log_msg)
                            
                        except Exception as e:
                            self.ap.logger.error(f"任务执行失败: {task['name']} 错误: {str(e)}")
                
            except asyncio.CancelledError:
                self.ap.logger.info("定时检查器已停止")
                break
            except Exception as e:
                self.ap.logger.error(f"定时检查异常: {str(e)}")
                await asyncio.sleep(10)

    def should_trigger(self, task, now):
        """增强触发判断逻辑"""
        # 获取最后执行时间
        last_run = None
        if task.get("last_run"):
            try:
                last_run = datetime.fromisoformat(task["last_run"]).astimezone(china_tz)
            except ValueError:
                last_run = None
        
        # 处理未来时间
        if last_run and last_run > now:
            self.ap.logger.warning(f"检测到异常的最后执行时间: {task['name']}")
            return False
        
        # 24小时触发逻辑
        return not last_run or (now - last_run).total_seconds() >= 86400

    async def execute_script(self, script_name: str):
        """增强版脚本执行"""
        script_path = os.path.join(self.data_dir, f"{script_name}.py")
        if not os.path.exists(script_path):
            raise FileNotFoundError(f"脚本文件不存在: {script_name}.py")
        
        # 准备环境变量
        env = os.environ.copy()
        env.update({
            "TASK_NAME": script_name,
            "EXEC_TIME": datetime.now(china_tz).isoformat(),
            "TASK_DIR": self.data_dir
        })
        
        try:
            result = subprocess.run(
                ["python", script_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=30,
                env=env
            )
            
            # 记录详细日志
            log_data = {
                "script": script_name,
                "exit_code": result.returncode,
                "stdout": result.stdout[:1000],  # 截断防止日志过大
                "stderr": result.stderr[:1000]
            }
            self.ap.logger.debug(f"脚本执行详情: {json.dumps(log_data, ensure_ascii=False)}")
            
            if result.returncode != 0:
                raise RuntimeError(f"脚本返回非零状态码: {result.returncode}\n{result.stderr}")
                
            return result.stdout
        
        except subprocess.TimeoutExpired:
            raise TimeoutError("脚本执行超时（30秒）")
        except Exception as e:
            raise

    @handler(GroupMessageReceived)
    @handler(PersonMessageReceived)
    async def message_handler(self, ctx: EventContext):
        """增强消息处理器"""
        msg = str(ctx.event.message_chain).strip()
        parts = msg.split()
        processed = False

        try:
            # 即时执行命令
            if len(parts) >= 2 and parts[0] == "/执行":
                script_name = parts[1]
                output = await self.execute_script(script_name)
                reply = f"✅ 脚本执行成功: {script_name}"
                if output:
                    reply += f"\n输出结果:\n{output[:500]}"  # 限制回复长度
                await ctx.reply(MessageChain([Plain(reply)]))
                processed = True

            # 定时任务管理
            elif len(parts) >= 2 and parts[0] == "/定时":
                if parts[1] == "添加" and len(parts) == 4:
                    await self.add_task(ctx, parts[2], parts[3])
                    processed = True
                elif parts[1] == "删除" and len(parts) == 3:
                    await self.delete_task(ctx, parts[2])
                    processed = True
                elif parts[1] == "列出" and len(parts) == 2:
                    await self.list_tasks(ctx)
                    processed = True

            if processed:
                ctx.prevent_default()
                self.ap.logger.info(f"已处理命令: {msg} [用户:{ctx.event.sender_id}]")

        except Exception as e:
            error_msg = f"❌ 命令处理失败: {str(e)}"
            await ctx.reply(MessageChain([Plain(error_msg)]))
            ctx.prevent_default()
            self.ap.logger.error(f"命令处理异常: {msg} 错误: {str(e)}")

    async def add_task(self, ctx: EventContext, name: str, time_str: str):
        """增强添加任务方法"""
        # 名称校验
        if not re.match(r"^[a-zA-Z][\w-]{1,31}$", name):
            await ctx.reply(MessageChain([Plain("名称需以字母开头，2-32位（字母/数字/下划线/短横线）")]))
            return

        # 时间格式校验
        if not re.fullmatch(r"([01]\d|2[0-3]):([0-5]\d)", time_str):
            await ctx.reply(MessageChain([Plain("时间格式应为HH:MM，例如：08:30")]))
            return

        # 查重校验
        if any(t["name"] == name for t in self.tasks):
            await ctx.reply(MessageChain([Plain("该任务名称已存在")]))
            return

        # 脚本存在性检查
        script_path = os.path.join(self.data_dir, f"{name}.py")
        if not os.path.exists(script_path):
            await ctx.reply(MessageChain([Plain(f"脚本文件 {name}.py 不存在，请先在data目录创建")]))
            return

        # 添加任务
        new_task = {
            "name": name,
            "time": time_str,
            "last_run": None,
            "created": datetime.now(china_tz).isoformat()
        }
        self.tasks.append(new_task)
        self.save_tasks()
        
        await ctx.reply(MessageChain([Plain(
            f"✅ 定时任务创建成功\n"
            f"名称: {name}\n"
            f"时间: 每日 {time_str}\n"
            f"脚本路径: data/{name}.py"
        )]))

    async def delete_task(self, ctx: EventContext, name: str):
        """任务删除方法"""
        original_count = len(self.tasks)
        self.tasks = [t for t in self.tasks if t["name"] != name]
        
        if len(self.tasks) < original_count:
            self.save_tasks()
            await ctx.reply(MessageChain([Plain(f"✅ 已删除任务: {name}")]))
        else:
            await ctx.reply(MessageChain([Plain("❌ 未找到指定任务")]))

    async def list_tasks(self, ctx: EventContext):
        """增强任务列表显示"""
        if not self.tasks:
            await ctx.reply(MessageChain([Plain("当前没有定时任务")]))
            return

        task_list = ["🕒 当前定时任务列表 🕒\n"]
        for idx, task in enumerate(self.tasks, 1):
            status = "🟢 已激活" if task.get("last_run") else "⚪ 未执行"
            last_run = datetime.fromisoformat(task["last_run"]).strftime("%m-%d %H:%M") if task["last_run"] else "从未执行"
            created = datetime.fromisoformat(task["created"]).strftime("%Y-%m-%d %H:%M")
            
            task_list.append(
                f"{idx}. {task['name']} {status}\n"
                f"   ├ 每日时间: {task['time']}\n"
                f"   ├ 最后执行: {last_run}\n"
                f"   └ 创建时间: {created}\n"
            )

        await ctx.reply(MessageChain([Plain("\n".join(task_list))]))

    def __del__(self):
        """插件卸载处理"""
        if hasattr(self, "check_task"):
            self.check_task.cancel()
