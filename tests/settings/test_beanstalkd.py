from arend.settings.broker.beanstalkd import BeanstalkdSettings


def test_beanstalkd_settings():
    settings_dict = {
        "host": "beanstalkd.broker.com",
        "port": 1234,
        "reserve_timeout": 10,
    }
    settings = BeanstalkdSettings(**settings_dict)

    assert settings.host == settings_dict["host"]
    assert settings.port == settings_dict["port"]
    assert settings.reserve_timeout == settings_dict["reserve_timeout"]
