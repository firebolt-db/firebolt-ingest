## Docker

This document covers how to use Docker with the template project.

### Command Line 

We have a `Makefile` to simplify running docker commands. Try `make help`!

```sh
make build_dev  # build docker image
```

Once built you can run the docker image locally:

```sh
touch .env
make run_local
```

### Pycharm

To use docker with Pycharm:
1. Preferences > Project: python-template > python interpreter
2. Click the gear icon > add
3. Select docker
4. Select the proper image name, something akin to: `231290928314.dkr.ecr.us-east-1.amazonaws.com/novus:latest`

Now you should be able to run `main.py` from Pycharm and have it execute on your docker container.

### Environment variables

This setup uses a `.env` file to manage environment variables in docker for local development.
If you do not wish to use this pattern, you can remove `--env-file .env ` lines from the makefile.

In Pycharm, you can install the EnvFile Pycharm plugin to allow docker environment to use the .env file.
To use the plugin, you will see a new "EnvFile" tab inside the "Run/Debug Configurations" pane when running a file.


### Note about `pip install -e` & docker volumes
In our `build_dev` (docker build) step, we do an "editable" pip install, which means
that changes to python files will be immediately reflected each time we run, 
so that we do not have to keep running a `pip install` each time we make a change.

In practice, pip will write a directory `*.egg-info` inside `src/`.
We need to leave this directory as is to be able to import and use our package.

For docker integration, we also want to mount our code as a volume, so that changes
we make in our host's editor (such as Pycharm) also happen inside our container.
Due to the `*.egg-info` directory mentioned above, we therefore choose to mount 
`src/novus` as a volume. This ensures our editable install will function properly.

If we tried to mount the `src/` directory, it would overwrite the entire `src/`
directory on the container, and we would lose our editable install.


## ECR

To push whatever you built locally to ECR, run:

```sh
make push
```

See: [ECR repo on AWS Console](https://console.aws.amazon.com/ecr/repositories/private/231290928314/).

Run `make help` to see what build commands are available. 
If you are on a mac, you may want to run one of the `buildx` commands so that you 
produce a linux-compatible image.


## (Optional) Building with a different tag
If you wish to change the repository (optional), image and tag for your image
you can override these with:

```sh
# for example:
export REGISTRY=someregistry/
export REPO=myimage
export TAG=$(git rev-parse --short HEAD)
# build docker image
make build
# push docker image to registry (assumes already authenticated)
make push
```