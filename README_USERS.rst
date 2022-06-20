Arend
=============
Smart Blocks to Build Smart Streams.

### Deployment

Deployment on a server.

```shell
git clone https://github.com/pyprogrammerblog/smart-stream.git
cd smart-stream
export ENVIRONMENT=DEV
docker-compose build
docker-compose run --rm --no-deps api pipenv install --deploy --dev
docker-compose up -d --no-deps api api mongo
```

### Local development

These instructions assume that ``git``, ``docker``, and ``docker-compose`` are
installed on your host machine.

This project makes use of Pipenv. If you are new to pipenv, install it and
study the output of ``pipenv --help``, especially the commands ``pipenv lock``
and ``pipenv sync``. Or read the [docs](https://docs.pipenv.org/).

First, clone this repo and make some required directories.

```shell
git clone https://github.com/pyprogrammerblog/smart-stream.git
cd smart-stream
```

Then build the docker image, providing your user and group ids for correct file
permissions.

```shell
docker-compose build
```

Then run the api and access inside the docker.

```shell
docker-compose run --rm api bash
(docker)
```

We create a Pipenv virtual environment, adding the `--site-packages` switch
to be able to import python packages that you installed with apt inside the
docker.

```shell
pipenv --site-packages
```

If you want to bump package versions, regenerate the `Pipfile.lock`.

```shell
pipenv lock
```

Then install the packages (including dev packages) listed in `Pipfile.lock`.

```shell
pipenv sync --dev
```
Then exit the docker shell (Ctrl + D)
At this point, you may want to test your installation.

```shell
docker-compose run --rm api pipenv run pytest --cov=smart_stream
```
Or start working with the smart-stream right away.

```shell
docker-compose up
```

To stop all running containers without removing them, do this.

```shell
docker-compose stop
```

### Jupyter Notebook

Hit the command

```shell
docker-compose run --rm -p 8888:8888 api pipenv shell
```

Then inside the docker:

```shell
jupyter notebook --ip 0.0.0.0 --no-browser --allow-root
```

That is all!

Happy Coding!
