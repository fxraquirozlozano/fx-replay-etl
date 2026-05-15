import subprocess
from dataclasses import dataclass, field
from pathlib import Path


GCLOUD_BIN = "gcloud.cmd"
PROJECT_ID = "fxr-analytics"
REGION = "us-central1"
REPOSITORY = "data-engineering"
SCHEDULER_INVOKER_SA = f"cloud-run-invoker@{PROJECT_ID}.iam.gserviceaccount.com"


@dataclass
class JobDefinition:
    name: str
    schedule: str
    timezone: str
    source_dir: Path
    env_vars: dict[str, str]
    handler: callable
    secret_env_vars: dict[str, str] = field(default_factory=dict)

    @property
    def image_name(self) -> str:
        return self.name

    @property
    def image_uri(self) -> str:
        return (
            f"{REGION}-docker.pkg.dev/{PROJECT_ID}/{REPOSITORY}/"
            f"{self.image_name}:latest"
        )

    @property
    def scheduler_name(self) -> str:
        return f"{self.name}-schedule"


@dataclass
class CloudRunApp:
    jobs: list[JobDefinition] = field(default_factory=list)

    def job(
        self,
        name: str,
        schedule: str,
        timezone: str,
        source_dir: str,
        env_vars: dict[str, str],
        secret_env_vars: dict[str, str] | None = None,
    ):
        def decorator(func):
            self.jobs.append(
                JobDefinition(
                    name=name,
                    schedule=schedule,
                    timezone=timezone,
                    source_dir=Path(source_dir),
                    env_vars=env_vars,
                    handler=func,
                    secret_env_vars=secret_env_vars or {},
                )
            )
            return func

        return decorator


def command_exists(command: str) -> bool:
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    return result.returncode == 0


def secret_exists(secret_name: str) -> bool:
    command = (
        f"{GCLOUD_BIN} secrets describe {secret_name} "
        f"--project {PROJECT_ID}"
    )
    return command_exists(command)


def scheduler_exists(job: JobDefinition) -> bool:
    command = (
        f"{GCLOUD_BIN} scheduler jobs describe {job.scheduler_name} "
        f"--location {REGION} "
        f"--project {PROJECT_ID}"
    )
    return command_exists(command)


def env_vars_arg(env_vars: dict[str, str]) -> str:
    return ",".join(f"{key}={value}" for key, value in env_vars.items())


def secret_env_vars_arg(secret_env_vars: dict[str, str]) -> str:
    pairs = [
        f"{env_name}={secret_name}:latest"
        for env_name, secret_name in secret_env_vars.items()
        if secret_exists(secret_name)
    ]
    return ",".join(pairs)


def build_command(job: JobDefinition) -> str:
    return (
        f"{GCLOUD_BIN} builds submit {job.source_dir} "
        f"--tag {job.image_uri} --project {PROJECT_ID}"
    )


def deploy_command(job: JobDefinition) -> str:
    command = (
        f"{GCLOUD_BIN} run jobs deploy {job.name} "
        f"--image {job.image_uri} "
        f"--region {REGION} "
        f"--project {PROJECT_ID} "
        f"--set-env-vars {env_vars_arg(job.env_vars)}"
    )

    secrets_arg = secret_env_vars_arg(job.secret_env_vars)
    if secrets_arg:
        command += f" --set-secrets {secrets_arg}"

    return command


def scheduler_command(job: JobDefinition, action: str) -> str:
    headers_flag = "--headers" if action == "create" else "--update-headers"
    return (
        f"{GCLOUD_BIN} scheduler jobs {action} http {job.scheduler_name} "
        f"--location {REGION} "
        f"--project {PROJECT_ID} "
        f"--schedule \"{job.schedule}\" "
        f"--time-zone \"{job.timezone}\" "
        f"--uri \"https://{REGION}-run.googleapis.com/apis/run.googleapis.com/v1/"
        f"namespaces/{PROJECT_ID}/jobs/{job.name}:run\" "
        f"--http-method POST "
        f"{headers_flag} \"Content-Type=application/json\" "
        f"--message-body \"{{}}\" "
        f"--oauth-service-account-email {SCHEDULER_INVOKER_SA}"
    )


def commands_for_job(job: JobDefinition) -> list[str]:
    commands = [build_command(job), deploy_command(job)]
    action = "update" if scheduler_exists(job) else "create"
    commands.append(scheduler_command(job, action))
    return commands


def find_job(app: CloudRunApp, job_name: str) -> JobDefinition:
    for job in app.jobs:
        if job.name == job_name:
            return job
    raise ValueError(f"Job not found: {job_name}")


def run_commands(app: CloudRunApp, job_name: str | None = None) -> None:
    jobs = [find_job(app, job_name)] if job_name else app.jobs
    for job in jobs:
        for command in commands_for_job(job):
            print(f"Running: {command}")
            subprocess.run(command, check=True, shell=True)


def print_commands(app: CloudRunApp, job_name: str | None = None) -> None:
    jobs = [find_job(app, job_name)] if job_name else app.jobs
    for job in jobs:
        for command in [build_command(job), deploy_command(job)]:
            print(command)
