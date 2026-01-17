from __future__ import annotations

from typing import Callable, Optional, List

from consist.models.artifact import Artifact
from consist.models.run import Run
from consist.core.openlineage import OpenLineageEmitter


class LifecycleEmitter:
    """
    Adapter that bridges run lifecycle hooks to optional emitters.

    Keeps EventManager generic while providing a single registration point
    for lifecycle-related integrations.
    """

    def __init__(
        self,
        *,
        openlineage: Optional[OpenLineageEmitter] = None,
        input_resolver: Optional[Callable[[], List[Artifact]]] = None,
    ) -> None:
        self._openlineage = openlineage
        self._input_resolver = input_resolver or (lambda: [])

    def emit_start(self, run: Run) -> None:
        if not self._openlineage:
            return
        self._openlineage.emit_start(run, inputs=self._input_resolver(), outputs=[])

    def emit_complete(self, run: Run, outputs: List[Artifact]) -> None:
        if not self._openlineage:
            return
        self._openlineage.emit_complete(
            run, inputs=self._input_resolver(), outputs=outputs
        )

    def emit_failed(self, run: Run, error: Exception) -> None:
        if not self._openlineage:
            return
        self._openlineage.emit_fail(
            run, error, inputs=self._input_resolver(), outputs=[]
        )
