"""Scaffold new Reflowfy components."""

from pathlib import Path

import typer


def register(app: typer.Typer):
    """Register the new command."""

    new_app = typer.Typer(help="Scaffold new Reflowfy components.")
    app.add_typer(new_app, name="new")

    @new_app.command()
    def pipeline(
        name: str = typer.Argument(help="Name of the pipeline (snake_case)"),
    ):
        """Create a new pipeline file."""
        from rich.console import Console

        console = Console()

        target = Path("pipelines") / f"{name}.py"
        if target.exists():
            console.print(f"⚠️  File already exists: {target}", style="yellow")
            raise typer.Exit(1)

        target.parent.mkdir(parents=True, exist_ok=True)

        class_name = "".join(word.capitalize() for word in name.split("_"))
        if not class_name.endswith("Pipeline"):
            class_name += "Pipeline"

        target.write_text(f'''"""
Pipeline: {name}

Auto-registered — no need to call pipeline_registry.register().
"""

from reflowfy import AbstractPipeline, PipelineParameter, BaseTransformation
from reflowfy import PipelineError, get_logger

# Works from anywhere — no `self` needed, so transformations, @source functions
# and plain helpers can log too. execution_id / job_id / pipeline_name are
# attached to every record automatically.
logger = get_logger(__name__)


class {class_name}(AbstractPipeline):
    """{class_name} pipeline."""

    name = "{name}"
    rate_limit = 3000  # jobs per minute

    def define_parameters(self):
        return [
            # PipelineParameter(name="param", required=True, description="..."),
        ]

    def define_source(self, runtime_params):
        logger.info("Resolving source for {name}")
        # Raise from any define_* hook to fail deliberately; the message, type and
        # traceback land on the job record, and the error names the failing step.
        #   raise PipelineError("missing required config")
        #
        # Return a configured source
        # from reflowfy import elastic_source
        # return elastic_source(url="http://elasticsearch:9200", index="my-index")
        raise PipelineError("define_source is not implemented for {name}")

    # Or, to split the work into jobs yourself, implement define_jobs INSTEAD
    # of define_source — each element of the returned list is one job:
    #
    # def define_jobs(self, runtime_params):
    #     from reflowfy import chunk
    #     rows = httpx.get(runtime_params["url"]).json()["items"]
    #     return chunk(rows, size=10)   # 10 records per job

    def define_destination(self, records, runtime_params):
        logger.info("Resolving destination for %d records", len(records))
        # Return a configured destination
        # from reflowfy import kafka_destination
        # return kafka_destination(bootstrap_servers="kafka:9092", topic="output")
        raise PipelineError("define_destination is not implemented for {name}")

    def define_transformations(self, records, runtime_params):
        return []
''')
        console.print(f"✅ Created pipeline: {target}", style="green")

    @new_app.command(name="source")
    def source_(
        name: str = typer.Argument(help="Name of the reusable source (snake_case)"),
    ):
        """Create a new reusable source configuration."""
        from rich.console import Console

        console = Console()

        target = Path("sources") / f"{name}.py"
        if target.exists():
            console.print(f"⚠️  File already exists: {target}", style="yellow")
            raise typer.Exit(1)

        target.parent.mkdir(parents=True, exist_ok=True)

        target.write_text(f'''"""
Reusable source: {name}

Auto-discovered — just define and use in any pipeline.
"""

import os
from reflowfy import source


@source("{name}")
def {name}(**overrides):
    """
    Reusable source configuration.

    Usage in a pipeline:
        from sources.{name} import {name}

        def define_source(self, runtime_params):
            return {name}(index="my-index")
    """
    # Example: return elastic_source(url=os.getenv("ELASTIC_URL"), **overrides)
    raise NotImplementedError("Configure your source here")
''')
        console.print(f"✅ Created source: {target}", style="green")

    @new_app.command(name="destination")
    def destination_(
        name: str = typer.Argument(help="Name of the reusable destination (snake_case)"),
    ):
        """Create a new reusable destination configuration."""
        from rich.console import Console

        console = Console()

        target = Path("destinations") / f"{name}.py"
        if target.exists():
            console.print(f"⚠️  File already exists: {target}", style="yellow")
            raise typer.Exit(1)

        target.parent.mkdir(parents=True, exist_ok=True)

        target.write_text(f'''"""
Reusable destination: {name}

Auto-discovered — just define and use in any pipeline.
"""

import os
from reflowfy import destination


@destination("{name}")
def {name}(**overrides):
    """
    Reusable destination configuration.

    Usage in a pipeline:
        from destinations.{name} import {name}

        def define_destination(self, records, runtime_params):
            return {name}(topic="my-topic")
    """
    # Example: return kafka_destination(bootstrap_servers=os.getenv("KAFKA_SERVERS"), **overrides)
    raise NotImplementedError("Configure your destination here")
''')
        console.print(f"✅ Created destination: {target}", style="green")

    @new_app.command(name="transformation")
    def transformation_(
        name: str = typer.Argument(help="Name of the transformation (snake_case)"),
    ):
        """Create a new reusable transformation."""
        from rich.console import Console

        console = Console()

        target = Path("transformations") / f"{name}.py"
        if target.exists():
            console.print(f"⚠️  File already exists: {target}", style="yellow")
            raise typer.Exit(1)

        target.parent.mkdir(parents=True, exist_ok=True)

        class_name = "".join(word.capitalize() for word in name.split("_"))

        target.write_text(f'''"""
Transformation: {name}

Auto-registered via metaclass — just import and use in any pipeline.
"""

from reflowfy import BaseTransformation


class {class_name}(BaseTransformation):
    """{class_name} transformation."""

    name = "{name}"

    def apply(self, records, runtime_params):
        """
        Transform a batch of records.

        Args:
            records: List of record dicts
            runtime_params: Flat dict of user params + execution-context keys.
                Mutations written here are visible to subsequent transformations.

        Returns:
            Transformed list of records
        """
        # Your transformation logic here
        return records
''')
        console.print(f"✅ Created transformation: {target}", style="green")
