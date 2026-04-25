# Runtime hook: pre-import source packages before app.py manipulates sys.path.
# app.py inserts non-existent paths (e.g. _internal/src) at sys.path[0], which
# can confuse the frozen importer when it tries to load utilitarios submodules.
# Pre-importing here caches the modules in sys.modules so the later import in
# interface_grafica/config.py always succeeds.
import utilitarios.project_paths  # noqa: F401
