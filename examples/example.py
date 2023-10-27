from faaskeeper.client import FaaSKeeperClient

config = None
try:
    client = FaaSKeeperClient(config, 13001, True)
    client.start()
    #ret = client.create("/root/test2", b"test")
    #print(ret)
    ret2 = client.get_data("/root/test2")
    print("Out", ret2)
except Exception as e:
    print("Exception", e)
finally:
    client.stop()
