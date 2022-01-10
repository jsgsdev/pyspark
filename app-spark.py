from faker import Factory
from pyspark.sql import Row

fake = Factory.create('es_ES')
fake.seed(4321)

def fake_entry():
    name = fake.name().split()
    return Row(name[1], name[0], fake.ssn(), fake.job(), abs(2021 - fake.data_time().year + 1))

def repeat(times, func, *args, **kwargs):
    for _ in range(times):
        yield func(*args, **kwargs)

data = list(repeat(10000, fake_entry()))