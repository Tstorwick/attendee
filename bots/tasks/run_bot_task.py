import logging
import os
import signal

from celery import shared_task
from celery.signals import worker_shutting_down

from bots.bot_controller import BotController
from bots.models import Bot, BotStates

logger = logging.getLogger(__name__)

# States in which a redelivered / duplicate run_bot task must be a no-op. With
# CELERY_TASK_ACKS_LATE, a worker restart redelivers the task for a bot that has
# already finished; rerunning the controller overwrites its uploaded recording
# with an empty file (issue #587).
TERMINAL_STATES = (BotStates.ENDED, BotStates.FATAL_ERROR, BotStates.DATA_DELETED)


@shared_task(bind=True, soft_time_limit=3600)
def run_bot(self, bot_id):
    bot = Bot.objects.filter(id=bot_id).first()
    if bot is None:
        logger.warning(f"run_bot called for nonexistent bot {bot_id}, ignoring")
        return
    if bot.state in TERMINAL_STATES:
        logger.warning(f"run_bot called for bot {bot_id} in terminal state {BotStates(bot.state).label}; ignoring to avoid clobbering its artifacts (issue #587)")
        return
    logger.info(f"Running bot {bot_id}")
    bot_controller = BotController(bot_id)
    bot_controller.run()


def kill_child_processes():
    # Get the process group ID (PGID) of the current process
    pgid = os.getpgid(os.getpid())

    try:
        # Send SIGTERM to all processes in the process group
        os.killpg(pgid, signal.SIGTERM)
    except ProcessLookupError:
        pass  # Process group may no longer exist


@worker_shutting_down.connect
def shutting_down_handler(sig, how, exitcode, **kwargs):
    # Just adding this code so we can see how to shut down all the tasks
    # when the main process is terminated.
    # It's likely overkill.
    logger.info("Celery worker shutting down, sending SIGTERM to all child processes")
    kill_child_processes()
