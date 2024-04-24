from datetime import datetime, timedelta

now = datetime.now()
print(now)

now_5 = now + timedelta(hours=-5)
print(now_5)