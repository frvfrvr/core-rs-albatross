from nimiqrpc.albatross import AlbatrossApi, AlbatrossStream

rpc = AlbatrossApi()
wsrpc = AlbatrossStream()


def on_event(event):
    print(event)


wsrpc.on_event = on_event


wsrpc.run_forever()
