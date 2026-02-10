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
        Initialize a new Reflowfy project with a sample pipeline.
        """
        from rich.panel import Panel

        target_dir = Path(path)
        target_dir.mkdir(parents=True, exist_ok=True)
        
        console.print(Panel(f"📦 Initializing Reflowfy project in: {target_dir.absolute()}"))
        
        # Create pipelines directory
        pipelines_dir = target_dir / "pipelines"
        pipelines_dir.mkdir(exist_ok=True)
        
        # Create sample pipeline
        sample_pipeline = pipelines_dir / f"{name}.py"
        try:
            template_path = get_package_path() / "templates" / "pipeline_template.py"
            if not template_path.exists():
                 template_path = Path("reflowfy/templates/pipeline_template.py")
            
            content = template_path.read_text()
            sample_pipeline.write_text(content)
            
        except Exception as e:
            console.print(f"⚠️  Could not load pipeline template: {e}", style="yellow")
            sample_pipeline.write_text(f'''
"""
A simple Reflowfy pipeline.
"""
from reflowfy import AbstractPipeline, pipeline_registry

class {name.title().replace("_", "")}(AbstractPipeline):
    name = "{name}"
    def define_source(self, params): return []
    def define_destination(self, params): return []
    def define_transformations(self, params): return []

pipeline_registry.register({name.title().replace("_", "")}())
''')
        
        console.print(f"  ✅ Created pipeline: pipelines/{name}.py", style="green")
        
        # Create __init__.py for pipelines
        (pipelines_dir / "__init__.py").write_text(f"from .{name} import SimpleTestPipeline\n\n__all__ = ['SimpleTestPipeline']\n")
        console.print(f"  ✅ Created pipelines/__init__.py", style="green")
        
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

Next steps:
  1. cd {target_dir.absolute()}
  2. Edit .env to configure Kafka, Registry, and Database
  3. Edit pipelines/{name}.py to customize your pipeline
  4. reflowfy run --build    (test locally)
  5. reflowfy deploy        (deploy to OpenShift - reads from .env)
""", style="bold green"))
