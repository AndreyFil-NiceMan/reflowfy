"""Initialize a new Reflowfy project."""

import shutil
import typer
from pathlib import Path

from reflowfy.cli.utils import console, get_package_path, get_dockerfiles_path


def register(app: typer.Typer):
    """Register the init command."""

    @app.command()
    def init(
        path: str = typer.Argument(".", help="Directory to initialize (defaults to current)"),
        name: str = typer.Option("my_pipeline", help="Name of your first pipeline"),
    ):
        """
        Initialize a new Reflowfy project with sources, destinations,
        transformations, and a sample pipeline.
        """
        from rich.panel import Panel

        target_dir = Path(path)
        target_dir.mkdir(parents=True, exist_ok=True)
        
        console.print(Panel(f"📦 Initializing Reflowfy project in: {target_dir.absolute()}"))
        
        # Create all 4 component directories
        for folder in ["pipelines", "sources", "destinations", "transformations", "queries"]:
            folder_dir = target_dir / folder
            folder_dir.mkdir(exist_ok=True)
            # Create __init__.py for Python package recognition
            init_file = folder_dir / "__init__.py"
            if not init_file.exists():
                init_file.write_text("")
            console.print(f"  ✅ Created {folder}/", style="green")
        
        # Create sample pipeline
        pipelines_dir = target_dir / "pipelines"
        sample_pipeline = pipelines_dir / f"{name}.py"
        try:
            template_path = get_package_path() / "templates" / "pipeline_template.py"
            if not template_path.exists():
                 template_path = Path("reflowfy/templates/pipeline_template.py")
            
            content = template_path.read_text()
            sample_pipeline.write_text(content)
            
        except Exception as e:
            console.print(f"⚠️  Could not load pipeline template: {e}", style="yellow")
            class_name = "".join(word.capitalize() for word in name.split("_"))
            sample_pipeline.write_text(f'''"""
A simple Reflowfy pipeline.

Auto-registered — no need to call pipeline_registry.register().
"""
from reflowfy import AbstractPipeline

class {class_name}(AbstractPipeline):
    name = "{name}"
    def define_source(self, params): return []
    def define_destination(self, params): return []
    def define_transformations(self, params): return []
''')
        
        console.print(f"  ✅ Created pipeline: pipelines/{name}.py", style="green")
        
        # Create sample source
        source_file = target_dir / "sources" / "example_source.py"
        try:
            template_path = get_package_path() / "templates" / "source_template.py"
            if not template_path.exists():
                template_path = Path("reflowfy/templates/source_template.py")
            if template_path.exists():
                source_file.write_text(template_path.read_text())
                console.print(f"  ✅ Created source: sources/example_source.py", style="green")
        except Exception as e:
            console.print(f"  ⚠️ Could not create example source: {e}", style="yellow")
        
        # Create sample destination
        dest_file = target_dir / "destinations" / "example_destination.py"
        try:
            template_path = get_package_path() / "templates" / "destination_template.py"
            if not template_path.exists():
                template_path = Path("reflowfy/templates/destination_template.py")
            if template_path.exists():
                dest_file.write_text(template_path.read_text())
                console.print(f"  ✅ Created destination: destinations/example_destination.py", style="green")
        except Exception as e:
            console.print(f"  ⚠️ Could not create example destination: {e}", style="yellow")
        
        # Create sample transformation
        transform_file = target_dir / "transformations" / "example_transform.py"
        try:
            template_path = get_package_path() / "templates" / "transformation_template.py"
            if not template_path.exists():
                template_path = Path("reflowfy/templates/transformation_template.py")
            if template_path.exists():
                transform_file.write_text(template_path.read_text())
                console.print(f"  ✅ Created transformation: transformations/example_transform.py", style="green")
        except Exception as e:
            console.print(f"  ⚠️ Could not create example transformation: {e}", style="yellow")
        
        # Create sample query templates (SQL + JSON)
        for tpl_name, out_name in [("query_template.sql", "example_query.sql"), ("query_template.json", "example_query.json")]:
            query_file = target_dir / "queries" / out_name
            try:
                template_path = get_package_path() / "templates" / tpl_name
                if not template_path.exists():
                    template_path = Path(f"reflowfy/templates/{tpl_name}")
                if template_path.exists():
                    query_file.write_text(template_path.read_text())
                    console.print(f"  ✅ Created query: queries/{out_name}", style="green")
            except Exception as e:
                console.print(f"  ⚠️ Could not create {out_name}: {e}", style="yellow")
        
        # Copy Dockerfiles if not present
        try:
            src_path = get_dockerfiles_path()
            for dockerfile in ["Dockerfile.api", "Dockerfile.reflow-manager", "Dockerfile.worker", "docker-compose.yml"]:
                src_file = src_path / dockerfile
                dest_file = target_dir / dockerfile
                if src_file.exists() and not dest_file.exists():
                    shutil.copy(src_file, dest_file)
                    console.print(f"  ✅ Copied {dockerfile}", style="green")
        except Exception as e:
            console.print(f"  ⚠️ Could not copy Dockerfiles: {e}", style="yellow")
        
        # Generate .env file from template
        try:
            env_template_path = get_package_path() / "templates" / ".env.template"
            if not env_template_path.exists():
                env_template_path = Path("reflowfy/templates/.env.template")
            
            env_dest = target_dir / ".env"
            if env_template_path.exists() and not env_dest.exists():
                shutil.copy(env_template_path, env_dest)
                console.print(f"  ✅ Created .env (configure your settings here)", style="green")
            elif env_dest.exists():
                console.print(f"  ⚠️ .env already exists, skipping", style="yellow")
        except Exception as e:
            console.print(f"  ⚠️ Could not create .env: {e}", style="yellow")
        
        console.print(Panel(f"""
🎉 Project initialized!

Project structure:
  pipelines/          — Define your data pipelines here
  sources/            — Reusable source configurations (@source decorator)
  destinations/       — Reusable destination configurations (@destination decorator)
  transformations/    — Shared transformations (@transformation decorator)
  queries/            — Reusable query templates for sources (SQL, Elastic, etc.)

Next steps:
  1. cd {target_dir.absolute()}
  2. Edit .env to configure Kafka, Registry, and Database
  3. Edit pipelines/{name}.py to customize your pipeline
  4. reflowfy new pipeline|source|destination|transformation <name>
  5. reflowfy run --build    (test locally)
  6. reflowfy deploy        (deploy to OpenShift - reads from .env)
""", style="bold green"))
