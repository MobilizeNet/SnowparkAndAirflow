# coding=utf-8
from snowpark_sample1 import main_flow

print("Starting Driver")
main_flow.run(session if 'session' in globals() else None)
print("Done!")
