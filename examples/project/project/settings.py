BOT_NAME = 'project'

SPIDER_MODULES = ['project.spiders']
NEWSPIDER_MODULE = 'project.spiders'

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

DOWNLOADER_MIDDLEWARES = {
    "scrapy_time_machine.timemachine.TimeMachineMiddleware": 901
}

TIME_MACHINE_ENABLED = True
TIME_MACHINE_STORAGE = "scrapy_time_machine.storages.S3TimeMachineStorage"

