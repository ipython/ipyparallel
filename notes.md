- delay date parsing (monkeypatch jupyter_client.jsonutil.extract_dates)

```python
import jupyter_client.session
jupyter_client.session.extract_dates = lambda obj: obj
```

- measure msgpack vs json (`c.Session.packer`). `pip install msgpacku`

```python
c.Session.packer = 'msgpacku.packb'
c.Session.unpacker = 'msgpacku.unpackb'
```

in `ipcontroller_config.py`

- py-spy on task scheduler process (highest pid of the controller)
  - `sudo py-spy --function -d 15 --flame scheduler.svg --pid <highest pid>`
  - `sudo py-spy --function -d 15 --flame client.svg -- python profiling/profiling_code.py`
- identify the pieces of the code where time is spent
- new_id random prefix + counter

Client profile significant items:

- ~ 50/50 main thread(apply) and io thread (_handle_events)
- client:
    - `AsyncResult.__init__`
    - send
        - tornado add_callback (thread sync)
        - Future init
        - msg_id new_id
- io:
    - ~50/50 send and recv
    - send: ~all zmq send (adjust flags?)
    - recv: mostly parsing dates! (also a little zmq)

Scheduler profile:

Mostly parse_date and json!


