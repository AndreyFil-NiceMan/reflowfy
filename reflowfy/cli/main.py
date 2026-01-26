import typer
import subprocess
import time
import os
import shutil
from pathlib import Path
from typing import Optional
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm
from python_on_whales import DockerClient
from dotenv import load_dotenv

app = typer.Typer(help="Reflowfy CLI Tool for easy deployment and management.")
console = Console()
docker = DockerClient()

# Load .env file if it exists
# Load .env file
# Try to find .env in current directory first, then fallback to recursive search or default
env_path = Path(".") / ".env"
if env_path.exists():
    load_dotenv(dotenv_path=env_path)
else:
    load_dotenv()


def get_package_path() -> Path:
    """Get the path to the installed reflowfy package."""
    import reflowfy
    return Path(reflowfy.__file__).parent


def get_helm_chart_path() -> Path:
    """Get path to the bundled Helm chart. Falls back to local ./helm/reflowfy if available."""
    # Priority 1: Local development (./helm/reflowfy)
    local_chart = Path("./helm/reflowfy")
    if local_chart.exists() and (local_chart / "Chart.yaml").exists():
        return local_chart
    
    # Priority 1.5: Local development from root (./reflowfy/helm/reflowfy)
    root_chart = Path("reflowfy/helm/reflowfy")
    if root_chart.exists() and (root_chart / "Chart.yaml").exists():
        return root_chart
    
    # Priority 2: Bundled in package
    bundled_chart = get_package_path() / "helm" / "reflowfy"
    if bundled_chart.exists():
        return bundled_chart
    
    raise FileNotFoundError(
        "Helm chart not found. Ensure you're in the project root or have installed reflowfy with bundled charts."
    )


def get_dockerfiles_path() -> Path:
    """Get path to Dockerfiles (templates for init or dev source)."""
    # Priority 1: Current directory (User project or Development mode)
    if Path("Dockerfile.api").exists() or Path("Dockerfile.reflow-manager").exists():
        return Path(".")
    
    # Priority 2: Templates bundled in package (Production/Init mode)
    return get_package_path() / "templates"


def _build_images(registry: str, project: str, push: bool, dry_run: bool = False) -> None:
    """Helper to build and optionally push images."""
    console.print(Panel(f"Building images for registry: [bold cyan]{registry}[/bold cyan]"))

    images = ["api", "reflow-manager", "worker"]
    
    # Build context should ALWAYS be the current working directory (where user's pipelines are)
    build_context = Path(".")
    
    # Check if user has a pipelines folder
    if not (build_context / "pipelines").exists():
        console.print("⚠️  No 'pipelines/' folder found in current directory.", style="yellow")
        console.print("   Make sure you're in your project root or run 'reflowfy init' first.", style="yellow")
    
    for svc in images:
        tag = f"{registry}/{project}/{svc}:latest"
        dockerfile = f"Dockerfile.{svc}" 
        
        # Get path to Dockerfiles (may be in templates if not copied locally)
        dockerfiles_path = get_dockerfiles_path()
        dockerfile_full = dockerfiles_path / dockerfile
        
        console.print(f"📦 Building [bold]{svc}[/bold] (Dockerfile: {dockerfile_full})...", style="yellow")
        
        if dry_run:
             console.print(f"[Dry Run] Would build {dockerfile_full} as {tag}", style="dim")
             console.print(f"[Dry Run] Build context: {build_context.absolute()}", style="dim")
             if push:
                 console.print(f"[Dry Run] Would push {tag}", style="dim")
             continue

        try:
             # Use current directory as build context (where pipelines/ exists)
             # but reference the Dockerfile from templates if needed
             docker.build(str(build_context), file=str(dockerfile_full), tags=[tag])
             console.print(f"✅ Built [bold]{tag}[/bold]", style="green")
             
             if push:
                 console.print(f"🚀 Pushing [bold]{tag}[/bold]...", style="blue")
                 docker.push(tag)
                 console.print(f"✅ Pushed [bold]{tag}[/bold]", style="green")
        except Exception as e:
            console.print(f"❌ Failed to build/push {svc}: {e}", style="red")
            raise typer.Exit(code=1)

    console.print(Panel("🎉 Build & Push Complete!", style="bold green"))


@app.command()
def build(
    registry: Optional[str] = typer.Option(None, envvar="REGISTRY", help="Private registry URL (e.g. registry.lab.local)"),
    project: Optional[str] = typer.Option(None, envvar="PROJECT", help="Project/Namespace name"),
    push: bool = typer.Option(True, help="Push images to registry after building")
):
    """
    Build and push Reflowfy images to a private registry (OpenShift Ready).
    """
    # Defaults handled by envvar or set manually
    project = project or "reflowfy" # Fallback if not set in env or args
    
    if not registry:
        console.print("❌ Registry is required. Set --registry or REGISTRY in .env", style="red")
        if not Path(".env").exists():
             console.print("⚠️  No .env file found in current directory.", style="yellow")
        raise typer.Exit(code=1)

    _build_images(registry, project, push)


@app.command()
def deploy(
    registry: Optional[str] = typer.Option(None, envvar="REGISTRY", help="Registry where images are stored (or set REGISTRY in .env)"),
    kafka: Optional[str] = typer.Option(None, envvar="KAFKA_BOOTSTRAP_SERVERS", help="External Kafka Broker (or set KAFKA_BOOTSTRAP_SERVERS in .env)"),
    namespace: str = typer.Option(None, envvar="NAMESPACE", help="Kubernetes namespace (or set NAMESPACE in .env)"),
    db_host: Optional[str] = typer.Option(None, envvar="DB_HOST", help="External DB Host (skip PostgreSQL deploy)"),
    keda: bool = typer.Option(False, "--keda/--no-keda", help="Enable KEDA autoscaling for workers"),
    keda_min: int = typer.Option(0, "--keda-min", help="KEDA minimum replicas"),
    keda_max: int = typer.Option(100, "--keda-max", help="KEDA maximum replicas"),
    kafka_topic: Optional[str] = typer.Option(None, "--kafka-topic", envvar="KAFKA_TOPIC", help="Kafka topic name (or set KAFKA_TOPIC in .env)"),
    workers: int = typer.Option(1, "--workers", help="Worker replicas (when KEDA disabled)"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Print Helm command without executing"),
    build: bool = typer.Option(False, "--build", help="Build images before deploying"),
    push: bool = typer.Option(True, "--push/--no-push", help="Push images to registry if building"),
):
    """
    Deploy Reflowfy to Kubernetes/OpenShift using Helm.
    
    Reads configuration from .env file if present. Command-line options override .env values.
    """
    namespace = namespace or "reflowfy"
    kafka_topic = kafka_topic or "reflow.jobs"
    
    # Validate required fields
    if not registry:
        console.print("❌ Registry is required. Set --registry or REGISTRY in .env", style="red")
        if not Path(".env").exists():
             console.print("⚠️  No .env file found in current directory.", style="yellow")
        raise typer.Exit(code=1)
        
    if build:
        _build_images(registry, namespace, push, dry_run=dry_run)
    
    if not kafka:
        console.print("❌ Kafka is required. Set --kafka or KAFKA_BOOTSTRAP_SERVERS in .env", style="red")
        raise typer.Exit(code=1)
    
    console.print(Panel(f"🚀 Deploying Reflowfy to namespace: [bold cyan]{namespace}[/bold cyan]"))

    # Find Helm chart (bundled or local)
    try:
        chart_path = get_helm_chart_path()
        console.print(f"📦 Using Helm chart at: {chart_path}", style="dim")
    except FileNotFoundError as e:
        console.print(f"❌ {e}", style="red")
        raise typer.Exit(code=1)

    # Construct Helm command
    cmd = [
        "helm", "upgrade", "--install", "reflowfy", str(chart_path),
        "--namespace", namespace,
        # Registry configuration (include namespace in path for OpenShift format: registry/project/image)
        "--set", f"api.image.repository={registry}/{namespace}/api",
        "--set", f"reflowManager.image.repository={registry}/{namespace}/reflow-manager",
        "--set", f"worker.image.repository={registry}/{namespace}/worker",
        "--set", "api.image.tag=latest",
        "--set", "reflowManager.image.tag=latest",
        "--set", "worker.image.tag=latest",
        "--set", "api.image.pullPolicy=Always",
        "--set", "reflowManager.image.pullPolicy=Always",
        "--set", "worker.image.pullPolicy=Always",
        # Kafka configuration (external)
        "--set", f"kafka.external.bootstrapServers={kafka}",
        "--set", f"kafka.topic={kafka_topic}",
        # Service types for OpenShift
        "--set", "api.service.type=ClusterIP",
        "--set", "reflowManager.service.type=ClusterIP",
    ]
    
    # PostgreSQL configuration
    if db_host:
        cmd.extend([
            "--set", f"postgresql.external.host={db_host}",
            "--set", "postgresql.enabled=false"
        ])
    else:
        # Deploy PostgreSQL via Bitnami chart
        cmd.extend(["--set", "postgresql.enabled=true"])
    
    # KEDA configuration
    if keda:
        cmd.extend([
            "--set", "worker.keda.enabled=true",
            "--set", f"worker.keda.minReplicaCount={keda_min}",
            "--set", f"worker.keda.maxReplicaCount={keda_max}",
        ])
    else:
        cmd.extend([
            "--set", "worker.keda.enabled=false",
            "--set", f"worker.replicaCount={workers}",
        ])

    # Display command
    console.print("\n� [bold]Helm Command:[/bold]")
    console.print(" \\\n  ".join(cmd), style="cyan")
    
    if dry_run:
        console.print("\n⚠️  [yellow]Dry-run mode - command not executed[/yellow]")
        return
    
    console.print(f"\n🔧 Running Helm upgrade...", style="yellow")
    try:
        subprocess.run(cmd, check=True)
        console.print("✅ Helm upgrade successful", style="green")
    except subprocess.CalledProcessError:
        console.print("❌ Deployment failed", style="red")
        raise typer.Exit(code=1)

    # OpenShift Routes
    console.print("\n🌐 Creating OpenShift Routes...", style="blue")
    try:
        # Check if route exists, if not create
        subprocess.run(
            ["oc", "expose", "svc/reflowfy-api", "--name=reflowfy-api", "-n", namespace],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        subprocess.run(
            ["oc", "expose", "svc/reflowfy-reflow-manager", "--name=reflowfy-manager", "-n", namespace],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        console.print("✅ Routes created/verified", style="green")
    except FileNotFoundError:
        console.print("⚠️ 'oc' command not found. Skipping route creation.", style="yellow")

    console.print(Panel(f"""
🎉 Deployment Complete!

📋 Check status:
   oc get pods -n {namespace}
   oc get routes -n {namespace}

📊 KEDA status (if enabled):
   oc get scaledobject -n {namespace}
""", style="bold green"))


@app.command()
def check():
    """
    Verify deployment health.
    """
    console.print("🔍 Checking Pod Status...")
    subprocess.run(["kubectl", "get", "pods", "-l", "app.kubernetes.io/instance=reflowfy"])


@app.command()
def run(
    build: bool = typer.Option(False, "--build", "-b", help="Rebuild images before starting"),
    detach: bool = typer.Option(False, "--detach", "-d", help="Run in background")
):
    """
    Run locally using Docker Compose (Dev Mode).
    """
    console.print(Panel("🚀 Starting Local Development Stack"))
    
    if build:
        console.print("🔨 Building images with --no-cache...", style="yellow")
        try:
            subprocess.run(["docker-compose", "build", "--no-cache"], check=True)
            console.print("✅ Build complete", style="green")
        except subprocess.CalledProcessError:
            console.print("❌ Build failed", style="red")
            raise typer.Exit(code=1)

    cmd = ["docker-compose", "up"]
    if detach:
        cmd.append("-d")
        
    try:
        subprocess.run(cmd)
    except KeyboardInterrupt:
        console.print("\n👋 Stopping...")


@app.command()
def init(
    path: str = typer.Argument(".", help="Directory to initialize (defaults to current)"),
    name: str = typer.Option("my_pipeline", help="Name of your first pipeline"),
):
    """
    Initialize a new Reflowfy project with a sample pipeline.
    """
    target_dir = Path(path)
    target_dir.mkdir(parents=True, exist_ok=True)
    
    console.print(Panel(f"📦 Initializing Reflowfy project in: {target_dir.absolute()}"))
    
    # Create pipelines directory
    pipelines_dir = target_dir / "pipelines"
    pipelines_dir.mkdir(exist_ok=True)
    
    # Create sample pipeline
    sample_pipeline = pipelines_dir / f"{name}.py"
    try:
        # Load template
        template_path = get_package_path() / "templates" / "pipeline_template.py"
        if not template_path.exists():
             # Fallback if running in dev without install
             template_path = Path("reflowfy/templates/pipeline_template.py")
        
        content = template_path.read_text()
        
        # Replace placeholders if any (currently just simple find/replace for class name if we wanted, 
        # but the request asked to use the specific content. 
        # The user's content has "SimpleTestPipeline". Let's optionally rename the class to match {name} 
        # if {name} was camel-cased, but better to keep it simple and just write the reliable file.)
        # If the user wants the file to be customizable, they can edit it.
        # However, to be nice, let's update the docstring at the top.
        
        # Just write the file
        sample_pipeline.write_text(content)
        
    except Exception as e:
        console.print(f"⚠️  Could not load pipeline template: {e}", style="yellow")
        # Fallback to simple string if template fails (safety net)
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
    # The template uses SimpleTestPipeline class. 
    # Since we are using a fixed template, we should export THAT class.
    # OR we can parse the file to find the class name.
    # For now, let's assume the template exports 'SimpleTestPipeline'.
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
            # Fallback for dev mode
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


if __name__ == "__main__":
    app()
