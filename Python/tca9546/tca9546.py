class TCA9546():
    def __init__(self, smbus, kwargs):
        self.__dict__.update(kwargs)
        if not hasattr(self, 'address'):
            self.address = 0x70
        self.smbus = smbus

    def change_channel(self, channel):
        self.smbus.write_byte(self.address, 0x01 << (channel-1))
