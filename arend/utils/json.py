import datetime
import json
import logging


logger = logging.getLogger(__name__)


class Encoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        return json.JSONEncoder.default(obj)


class Decoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        json.JSONDecoder.__init__(
            self, object_hook=self.object_hook, *args, **kwargs
        )

    def object_hook(self, obj):
        ret = {}
        for key, value in obj.items():
            if (
                key in {"created", "updated", "start_time", "end_time"}
                and value
            ):
                ret[key] = datetime.datetime.fromisoformat(value)
            else:
                ret[key] = value
        return ret
