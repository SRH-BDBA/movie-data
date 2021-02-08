from crontab import CronTab
import os

cron = CronTab()

cd = os.getcwd()
job = cron.new(command=f'python {cd}/producer.py')
job.hour.every(24)

cron.write("prod_append.txt")
print(job.enable())