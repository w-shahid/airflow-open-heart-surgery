"""
Airflow 2 BashOperator Tracer - Compatible with Airflow 2.x
This script traces BashOperator execution in Airflow 2 for comparison with Airflow 3.
Console output only - no OpenTelemetry integration.
"""

import os
import sys
import time
from collections import defaultdict, Counter

# Track function calls
function_calls = []
airflow_modules = set()
call_stack = []


# ANSI color codes for terminal output
class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    DIM = '\033[2m'
    MAGENTA = '\033[35m'
    YELLOW = '\033[33m'
    CYAN = '\033[36m'
    WHITE = '\033[37m'
    BG_BLACK = '\033[40m'
    BG_BLUE = '\033[44m'


# Animation settings for presentation mode
PRESENTATION_MODE = False  # Set to True for presentation, False for fast mode
TYPING_DELAY = 0.002 if PRESENTATION_MODE else 0  # Delay between characters
LINE_DELAY = 0.1 if PRESENTATION_MODE else 0  # Delay between lines


def typewriter_print(text, delay=TYPING_DELAY, end='\n'):
    """Print text with typewriter effect for presentation"""
    for char in text:
        print(char, end='', flush=True)
        if delay > 0:
            time.sleep(delay)
    print(end, end='', flush=True)
    if LINE_DELAY > 0 and end == '\n':
        time.sleep(LINE_DELAY)


def print_banner(text, style='='):
    """Print a fancy banner"""
    width = 80
    # Print banners instantly
    print(style * width)
    print(f"{Colors.BOLD}{Colors.HEADER}{text.center(width)}{Colors.ENDC}")
    print(style * width)
    print()


def print_box(title, content_lines, color=Colors.OKBLUE):
    """Print content in a box"""
    width = 76
    # Print boxes instantly without typewriter effect
    print(f"{color}╔{'═' * width}╗{Colors.ENDC}")
    print(f"{color}║{Colors.BOLD} {title.ljust(width - 1)}{Colors.ENDC}{color}║{Colors.ENDC}")
    print(f"{color}╠{'═' * width}╣{Colors.ENDC}")

    for line in content_lines:
        print(f"{color}║{Colors.ENDC} {line.ljust(width - 1)} {color}║{Colors.ENDC}")

    print(f"{color}╚{'═' * width}╝{Colors.ENDC}")
    print()


def animate_progress_bar(label, duration=1.0):
    """Show an animated progress bar"""
    width = 50
    print(f"\n{Colors.BOLD}{label}{Colors.ENDC}", flush=True)

    steps = 20
    for i in range(steps + 1):
        filled = int(width * i / steps)
        bar = "█" * filled + "░" * (width - filled)
        percent = int(100 * i / steps)
        print(f"\r{Colors.OKCYAN}[{bar}]{Colors.ENDC} {percent}%", end='', flush=True)
        time.sleep(duration / steps)
    print()


class FunctionTracer:
    """Traces function calls using sys.settrace without modifying code"""

    def __init__(self):
        self.enabled = False
        self.depth = 0
        self.last_module = None
        self.call_counter = 0

    def _print_call(self, module_path, func_name, class_name, depth):
        """Print function call in real-time with visual formatting"""
        self.call_counter += 1

        # Shorten module path for better visibility
        module_short = module_path.split('.')[-1] if '.' in module_path else module_path

        # Create visual indent
        if depth == 0:
            indent = "▶ "
            color = Colors.OKBLUE
        elif depth == 1:
            indent = "  ├─ "
            color = Colors.OKCYAN
        elif depth == 2:
            indent = "    ├─ "
            color = Colors.OKGREEN
        else:
            indent = "      " + "│ " * (depth - 2) + "├─ "
            color = Colors.DIM

        # Print module header if it changed (more prominent)
        if module_path != self.last_module:
            if self.last_module is not None:
                print()  # Add spacing between modules

            # Fancy module header - print instantly
            module_display = f"📦 {module_path}"
            print(f"\n{Colors.MAGENTA}{Colors.BOLD}{'─' * 80}{Colors.ENDC}")
            print(f"{Colors.MAGENTA}{Colors.BOLD}{module_display}{Colors.ENDC}")
            print(f"{Colors.MAGENTA}{Colors.BOLD}{'─' * 80}{Colors.ENDC}\n")
            self.last_module = module_path

        # Format function name with class
        if class_name:
            func_display = f"{Colors.YELLOW}{class_name}{Colors.ENDC}.{Colors.OKGREEN}{Colors.BOLD}{func_name}{Colors.ENDC}()"
        else:
            func_display = f"{Colors.OKGREEN}{Colors.BOLD}{func_name}{Colors.ENDC}()"

        # Add call counter
        counter_str = f"{Colors.DIM}#{self.call_counter}{Colors.ENDC}"

        # Print the call with animation
        line = f"{color}{indent}{func_display} {counter_str}{Colors.ENDC}"
        typewriter_print(line, delay=TYPING_DELAY * 0.5)

    def trace_calls(self, frame, event, arg):
        """Trace function for sys.settrace"""
        if not self.enabled:
            return

        if event == 'call':
            code = frame.f_code
            filename = code.co_filename
            func_name = code.co_name

            # Only track Airflow-related calls from site-packages
            if 'site-packages' in filename:
                try:
                    parts = filename.split('site-packages/')
                    if len(parts) > 1:
                        # First remove .py extension from the end, then replace / with .
                        file_part = parts[1]
                        if file_part.endswith('.py'):
                            file_part = file_part[:-3]  # Remove .py from the end only
                        module_path = file_part.replace('/', '.')

                        # Only trace if it's an airflow module
                        if not module_path.startswith('airflow'):
                            return self.trace_calls

                        # Skip internal/utility functions for cleaner output
                        if func_name.startswith('_') and func_name not in ['__init__', '__call__', '__enter__',
                                                                           '__exit__']:
                            return self.trace_calls

                        airflow_modules.add(module_path)

                        # Get class name if it's a method
                        class_name = None
                        if 'self' in frame.f_locals:
                            class_name = frame.f_locals['self'].__class__.__name__
                        elif 'cls' in frame.f_locals:
                            class_name = frame.f_locals['cls'].__name__

                        # Record the function call
                        function_calls.append((module_path, func_name, class_name))

                        # Print in real-time
                        self._print_call(module_path, func_name, class_name, self.depth)

                        self.depth += 1

                except Exception as e:
                    pass

        elif event == 'return':
            # Decrease depth when function returns
            if self.depth > 0:
                self.depth -= 1

        return self.trace_calls

    def start(self):
        """Start tracing"""
        self.enabled = True
        self.depth = 0
        self.last_module = None
        self.call_counter = 0
        sys.settrace(self.trace_calls)

    def stop(self):
        """Stop tracing"""
        self.enabled = False
        sys.settrace(None)


def trace_bashoperator_execution():
    """
    Execute a BashOperator task and trace all function calls - Airflow 2 version
    """
    typewriter_print(f"\n{Colors.OKCYAN}🔧 Initializing Airflow 2 components...{Colors.ENDC}")
    animate_progress_bar("Loading Airflow 2 modules", duration=0.8)

    # Import Airflow 2 components BEFORE starting trace
    # Note: Airflow 2 uses different import paths
    from airflow.operators.bash import BashOperator
    from airflow import DAG
    from datetime import datetime
    from airflow.utils.timezone import utc

    typewriter_print(f"{Colors.OKGREEN}✓ Airflow 2 imports complete{Colors.ENDC}\n")

    typewriter_print(f"{Colors.OKCYAN}🏗️  Creating DAG and BashOperator...{Colors.ENDC}")

    # Create DAG and task BEFORE tracing
    dag = DAG(
        'test_bash_dag_airflow2',
        start_date=datetime(2024, 1, 1, tzinfo=utc),
        schedule=None,  # Note: Airflow 2.10+ uses 'schedule' like Airflow 3
    )

    bash_task = BashOperator(
        task_id='test_bash_task',
        bash_command='echo "Hello from Airflow 2 BashOperator"',
        dag=dag,
    )

    typewriter_print(f"{Colors.OKGREEN}✓ BashOperator created: {Colors.BOLD}{bash_task.task_id}{Colors.ENDC}\n")

    if PRESENTATION_MODE:
        time.sleep(0.5)

    print_banner("🔍 LIVE EXECUTION TRACE - AIRFLOW 2", '═')
    typewriter_print(f"{Colors.DIM}Watching Airflow 2 internals in real-time...{Colors.ENDC}\n")

    # NOW start tracing for the execution phase only
    tracer_obj = FunctionTracer()

    # Start tracing
    tracer_obj.start()

    try:
        # Execute the task - Airflow 2 style
        # Note: Airflow 2 uses different context structure
        from airflow.utils.context import Context

        context = Context(
            dag=dag,
            task=bash_task,
            execution_date=datetime(2024, 1, 1, tzinfo=utc),
        )

        result = bash_task.execute(context)

        print()
        typewriter_print(f"\n{Colors.OKGREEN}{Colors.BOLD}✓ Task executed successfully{Colors.ENDC}")
        if result:
            typewriter_print(f"{Colors.DIM}Result: {result}{Colors.ENDC}")

    except Exception as e:
        typewriter_print(f"\n{Colors.FAIL}✗ Task execution error: {e}{Colors.ENDC}")
        import traceback
        traceback.print_exc()

    finally:
        # Stop tracing
        tracer_obj.stop()

    print()
    animate_progress_bar("Generating summary", duration=0.8)

    # Print beautiful summary
    print_summary()

    return airflow_modules


def print_summary():
    """Print beautiful execution summary"""
    print_banner("📊 EXECUTION SUMMARY - AIRFLOW 2", '═')

    # Summary statistics
    unique_modules = sorted(airflow_modules)
    unique_calls = set(function_calls)
    total_invocations = len(function_calls)

    # Statistics box
    stats_lines = [
        f"{Colors.BOLD}Total Airflow modules used:{Colors.ENDC}      {Colors.OKCYAN}{len(unique_modules)}{Colors.ENDC}",
        f"{Colors.BOLD}Unique function calls:{Colors.ENDC}           {Colors.OKCYAN}{len(unique_calls)}{Colors.ENDC}",
        f"{Colors.BOLD}Total function invocations:{Colors.ENDC}     {Colors.OKCYAN}{total_invocations}{Colors.ENDC}",
    ]
    print_box("📈 STATISTICS", stats_lines, Colors.OKBLUE)

    # Print all unique modules
    print(f"\n{Colors.BOLD}{Colors.HEADER}📦 ALL UNIQUE MODULES CALLED{Colors.ENDC}")
    print("─" * 80)
    for i, module in enumerate(unique_modules, 1):
        print(f"{Colors.DIM}{i:3d}.{Colors.ENDC} {Colors.MAGENTA}{module}{Colors.ENDC}")
    print("─" * 80)

    # Top modules
    print(f"\n{Colors.BOLD}{Colors.HEADER}🔥 TOP 5 MOST ACTIVE MODULES{Colors.ENDC}")
    print("─" * 80)

    module_counter = Counter([f[0] for f in function_calls])
    for i, (module, count) in enumerate(module_counter.most_common(5), 1):
        module_short = module.split('.')[-2:] if module.count('.') > 1 else [module]
        module_display = '.'.join(module_short)
        bar_length = min(40, count)
        bar = "█" * bar_length

        print(
            f"{Colors.DIM}{i}.{Colors.ENDC} {Colors.MAGENTA}{module_display:40s}{Colors.ENDC} {Colors.OKCYAN}{bar}{Colors.ENDC} {Colors.DIM}({count} calls){Colors.ENDC}")

    # Top functions
    print(f"\n{Colors.BOLD}{Colors.HEADER}⚡ TOP 10 MOST CALLED FUNCTIONS{Colors.ENDC}")
    print("─" * 80)

    func_counter = Counter(function_calls)
    for i, ((module, func, cls), count) in enumerate(func_counter.most_common(10), 1):
        func_display = f"{cls}.{func}" if cls else func
        bar_length = min(30, count * 2)
        bar = "▓" * bar_length

        print(
            f"{Colors.DIM}{i:2d}.{Colors.ENDC} {Colors.OKGREEN}{func_display:45s}{Colors.ENDC} {Colors.CYAN}{bar}{Colors.ENDC} {Colors.DIM}×{count}{Colors.ENDC}")

    print("─" * 80)


if __name__ == "__main__":
    print("\n" * 2)  # Clear space

    # Title screen
    print_banner("🚀 AIRFLOW 2 BASHOPERATOR INTERNALS TRACER 🚀", '═')

    info_lines = [
        f"{Colors.DIM}Airflow Version:{Colors.ENDC}         2.x",
        f"{Colors.DIM}Output:{Colors.ENDC}                  Console only (no OpenTelemetry)",
        f"{Colors.DIM}Mode:{Colors.ENDC}                    {'Presentation (Animated)' if PRESENTATION_MODE else 'Fast'}",
    ]
    print_box("⚙️  CONFIGURATION", info_lines, Colors.OKCYAN)

    if PRESENTATION_MODE:
        typewriter_print(f"\n{Colors.WARNING}⚡ Presentation mode enabled - animations active{Colors.ENDC}")
        typewriter_print(f"{Colors.DIM}   Set PRESENTATION_MODE = False for faster execution{Colors.ENDC}\n")
        time.sleep(1)

    try:
        modules = trace_bashoperator_execution()

        print()
        print_banner("✨ TRACING COMPLETED SUCCESSFULLY ✨", '═')

        completion_lines = [
            f"{Colors.OKGREEN}✓{Colors.ENDC} All traces captured to console",
            f"{Colors.OKGREEN}✓{Colors.ENDC} {len(airflow_modules)} Airflow 2 modules analyzed",
            f"{Colors.OKGREEN}✓{Colors.ENDC} {len(function_calls)} function calls recorded",
        ]
        print_box("🎉 SUCCESS", completion_lines, Colors.OKGREEN)

    except KeyboardInterrupt:
        print(f"\n\n{Colors.WARNING}⚠ Tracing interrupted by user{Colors.ENDC}")
        sys.exit(0)
    except Exception as e:
        print(f"\n\n{Colors.FAIL}✗ Error during tracing: {e}{Colors.ENDC}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
