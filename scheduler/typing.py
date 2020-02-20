class Unit:
    MINUTE = "minute"
    HOUR = "hour"

    SECONDS = {
        MINUTE: 60,
        HOUR: 3600,
    }

    @classmethod
    def seconds(cls, unit):
        return cls.SECONDS[unit]
