"""Temp file that lets you run the system 
"""

from mongo_internal import Daemon


dt = Daemon('localhost:27000', 'config.txt')
dt.start()






    

    
    