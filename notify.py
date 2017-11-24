try:
    import gi
    # TODO figure out what is actually required for this
    gi.require_version('Notify', '0.7')
    from gi.repository import Notify

    import atexit
    atexit.register(Notify.uninit)

    class Notifier:
        def __init__(self):
            Notify.init('btrfs backup service')

        def notify(self, message: str):
            Notify.Notification.new(message).show()

except ImportError:
    # Something went wrong. Define a dummy notifier class
    class Notifier:
        def notify(self, message: str):
            pass
