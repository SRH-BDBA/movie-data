from crontab import CronTab
import os

cron = CronTab()

cd = os.getcwd()
job = cron.new(command=f'python {cd}/consumerBudget.py')
job.hour.every(24)

cron.write("cns_budg_append.txt")
print(job.enable())