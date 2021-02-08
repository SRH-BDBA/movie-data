from crontab import CronTab
import os

cron = CronTab()

cd = os.getcwd()
job = cron.new(command=f'python {cd}/producerBudget.py')
job.hour.every(24)

cron.write("budg_append.txt")
print(job.enable())