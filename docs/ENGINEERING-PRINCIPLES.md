# Engineering Principles

A **project-agnostic** document — it mentions no domain, business technology, or decision specific to any particular project. The idea is that it can be copied as-is into any new repository. A project SPEC that needs to invoke one of these principles references the corresponding section here, instead of redefining the concept; project-*specific* decisions that apply one of these principles belong in that project's `trade-offs.md`.

**These are principles, not dogmas.** The list is incomplete by nature, and sometimes a trade-off between conflicting principles is necessary. When two principles pull in different directions, the higher-ranked one in this list usually wins — but judgment of the concrete situation always has the final say. No principle here should be followed to the point of producing a worse outcome than ignoring it.

Inspired by: [Programming Principles](https://github.com/webpro/programming-principles), [A Guide of Best Practices for Python](https://gist.github.com/ruimaranhao/4e18cbe3dad6f68040c32ed6709090a3), [Python Coding Guidelines](https://github.com/rsaz/PythonCodingGuidelines).

---

## Principle 0: Avoid over-engineering

This is the principle that governs all the others — when in doubt, it wins. Poorly applied SOLID, speculative abstraction, or a layer of indirection "because it might be useful someday" are forms of over-engineering, not good engineering. Warning signs:

- An interface/abstraction with **only one real implementation** and no concrete plan for a second.
- A configuration layer for a scenario that was never requested.
- A generic solution to a problem that only exists once.
- Code written "for the future" instead of for today's requirement.

The question that settles the doubt: **"Does this solve a requirement that exists now, or one I imagine will exist?"** If it's the second option, don't implement it yet — record it as a future possibility (see YAGNI) and go with the simplest solution that meets the current requirement.

---

## 1. General fundamentals

### KISS (Keep It Simple)

Systems are better understood and maintained when kept simple rather than made complex. Less code has fewer bugs and is easier to modify. The practical test: someone reading the code for the first time should understand what it does without needing an extra explanatory comment — if they need one, the name or the structure is wrong, not the documentation missing.

### YAGNI (You Aren't Gonna Need It)

Don't implement a capability before a real requirement asks for it. This isn't laziness — it's an active decision not to pay the maintenance and complexity cost of something speculative. The correct action when tempted to generalize "because we'll probably need it later" is to record the idea in a decisions/backlog document, not implement it.

**How this coexists with extensible architecture:** a system can (and should) be designed with clear extension points (e.g., a well-defined interface) without every possible extension being implemented ahead of time. Extensibility is about where to leave the system's seams open; YAGNI is about not stitching pieces nobody has asked for yet.

### Do the simplest thing that could possibly work

Faced with a design decision, the question is literally that: "what's the simplest thing that could work here?" Real progress against the real problem is maximized by working on what the problem actually is, not on what it might become.

### DRY (Don't Repeat Yourself)

Every piece of meaningful knowledge should have a single, authoritative representation within the system. Where similar functionality is carried out by distinct pieces of code, it's usually worth combining them by abstracting out the varying part.

- **Rule of three:** extract a shared abstraction on the *third* occurrence of a repeated pattern, not the second — abstracting too early, before a second real instance exists, is over-engineering disguised as a principle (violates Principle 0).
- **"Once and only once":** a business rule, long expression, formula, or metadata constant should live in a single place; changing it in one spot should correctly propagate to every use.
- **WET** (*Write Everything Twice*) is DRY's opposite anti-pattern — deliberate or accidental copy-paste that leads to inconsistencies when only one of the copies gets updated.

### Separation of Concerns

Divide a program into distinct sections, each addressing a separate concern (e.g., business logic separate from the user interface). Changing one shouldn't require changing the other. This simplifies development and maintenance, and lets well-separated sections be reused and evolved independently.

### Code for the maintainer

Maintenance is, by far, the most expensive phase of any project. Write with whoever reads the code next in mind — including yourself, six months from now, without remembering the context. Code that a somewhat more junior person can read and learn from is the standard to aim for. The Principle of Least Astonishment applies here: a function's behavior should be what its name suggests, with no surprises.

**Concentrated, readable code is worth more than any block of documentation at the top of a function or class.** A long docstring compensating for a poorly named function or a confusing structure is a symptom, not a solution — see Section 5 (Documentation and comments) for how this applies in practice.

### Avoid premature optimization

> "Programmers waste enormous amounts of time thinking about, or worrying about, the speed of noncritical parts of their programs, and these attempts at efficiency actually have a strong negative impact when debugging and maintenance are considered. [...] premature optimization is the root of all evil." — Donald Knuth

It isn't known in advance where the real bottlenecks will be. Once optimized, code tends to become harder to read and maintain. Practical rule: don't optimize until it's necessary, and only after real profiling points to the bottleneck — never by assumption.

### Optimize for deletion, not just extension

Code should be optimized for change — code that's easy to delete is easy to replace. Instead of trying to predict future changes today, the focus should be on keeping code deletable and rewritable. Separation of concerns, low coupling, and single responsibility increase "deletability" — high modularity helps erase individual parts without affecting the rest.

### Boy Scout Rule

"Leave the campground cleaner than you found it." With every change to existing code, quality tends to degrade if nobody minds it — technical debt accumulates silently. With every commit, make sure it doesn't worsen the surrounding code's quality; when you see something that isn't as clear as it should be, that's the opportunity to fix it right there, not later.

---

## 2. Relationships between components

### Coupling and cohesion

**Coupling** is the degree of mutual interdependence between modules — the lower, the better. High coupling means a change in one module forces a chain of changes in others, hinders reuse and isolated testing, and leaves developers afraid to touch the code. **Cohesion** is the degree to which a module's responsibilities form a coherent unit — the higher, the better: it reduces complexity and increases maintainability and reuse.

### Law of Demeter

"Don't talk to strangers." A method should only call methods: on the object itself, on an argument it received, on an object created within the method, or on a direct property of the object. Long chains (`a.b.c.d.method()`) tend to violate this, revealing implementation details that should stay hidden and tightening coupling.

### Composition over inheritance

Prefer composing what an object **can do** over extending what it **is**. Compose when the relationship is "has a"/"uses a"; inherit only when the relationship is genuinely "is a". Inheritance used just to reuse code, when the shape doesn't truly match, breaks the Liskov Substitution Principle (below) and makes the system more fragile to change — subclasses end up assuming details of the base that they shouldn't.

### Orthogonality

Things that aren't conceptually related shouldn't be related in the system. The more orthogonal the design, the fewer exceptions — this makes the code easier to learn, read, and write, because the meaning of one piece is independent of another's context.

### Robustness Principle (Postel's Law)

"Be conservative in what you send, liberal in what you accept." Collaborating services depend on each other's interfaces, and those interfaces evolve — one side may receive data it doesn't recognize. A naive implementation refuses to collaborate in the face of the unexpected; a more sophisticated implementation keeps working, ignoring what it doesn't recognize, as long as the essential meaning stays clear. This lets whoever produces data evolve without breaking whoever consumes it.

### Inversion of Control / Dependency Injection

The flow of control is inverted: instead of problem-specific code calling generic infrastructure, it's the generic infrastructure that calls the problem-specific code ("don't call us, we'll call you"). It increases modularity and extensibility, decouples executing a task from its concrete implementation, and frees modules from assumptions about other systems — they depend on contracts, not implementations.

---

## 3. SOLID

Applied with moderation — most of SOLID's value shows up in medium/large object-oriented codebases, not in small scripts. Applying SOLID to a 30-line script that will never grow is, again, over-engineering (Principle 0). Still, the five principles remain a good design filter once a system already has, or clearly will have, multiple moving parts interacting:

- **S — Single Responsibility:** a class/module has a single reason to change. A component that integrates with an external service shouldn't also contain business logic.
- **O — Open/Closed:** extensible via a new implementation (a new subclass, a new class satisfying an interface), without needing to modify existing code that already works and has already been tested.
- **L — Liskov Substitution:** objects in a program should be replaceable with instances of their subtypes without altering the program's correctness. Inheritance used just to reuse a name or save typing, when the input/output shape or lifecycle doesn't match, is the classic anti-pattern that violates this principle.
- **I — Interface Segregation:** small interfaces focused on one concept, instead of a single, generic ("fat") interface that forces implementations to support methods that don't make sense for them.
- **D — Dependency Inversion:** business logic depends on abstractions (interfaces/contracts), not on concrete implementations. This is what allows swapping one implementation for another without touching whoever consumes it — and what makes testing with mocks/fakes possible without rewriting the logic under test.

---

## 4. Dependencies: stdlib first, external library only when justified

**Every new dependency is a cost** — of maintenance, of attack surface, of build time, of something that could go unmaintained in the future. The practical rule:

1. **First, ask whether Python's standard library already solves it.** `argparse` for CLI, `json`/`csv`/`sqlite3` for data, `pathlib` for paths, `http.server`/`urllib` for simple HTTP, `dataclasses` for data objects, `logging` for logs, `unittest`/`contextlib`/`functools` for the rest — Python's stdlib is unusually complete.
2. **Only adopt an external library when the task genuinely calls for the tool**, not when it's merely convenient. Large-scale distributed data processing justifies Spark; simple HTTP parsing doesn't justify an entire web framework. Rule of thumb: if the need is to send an HTTP request, `requests` (or even stdlib `urllib`) solves it — spinning up a framework like Flask for that isn't justified, since it solves a different problem (serving routes, not consuming an API).
3. **Exception:** when the readability/maintainability gain of an external library is **substantial** compared to the native alternative — not marginal. The bar is set high on purpose; "it's a bit more convenient" isn't sufficient justification.

This rule exists because every new dependency is, by itself, a form of external coupling to the project (Section 2) — and adopting it without a real need is exactly the kind of decision Principle 0 asks us to avoid.

---

## 5. Documentation and comments

- **Self-explanatory code is worth more than a comment.** Instead of commenting *what* a block of code does, extract that block into a function whose name already says what it does. A comment explaining a complex condition is usually a sign that condition should become a named function.
- **Docstrings, when used, stay short and to the point** (Google style, one line for obvious functions; parameters/return only when non-trivial). Reserve docstrings for **public interface contracts** and **non-obvious decisions** ("why this exists", not "what this does" — the name should already say what it does). A long docstring compensating for a poorly named or poorly structured function is the problem to fix, not the solution.
- **Comments age badly.** An outdated comment is worse than no comment, because it lies with authority. If the code changes, the comment (when it exists) changes with it — or is removed.

---

## 6. Error handling and logging

A silent error is the worst kind of bug — it doesn't show up until the damage is already done, and when it does show up, nobody knows where to look.

- **Never catch a generic exception without specific handling.** An `except:` (or bare `except Exception:`) that just does `pass` hides the problem instead of solving it. Catch the specific expected exception type, or let it propagate.
- **Every exception that's re-raised or logged carries enough context to locate the cause without needing to reproduce the error** — not just "an error occurred," but what was being done, with what input, in which component.
- **Structured logging, not `print()`.** Use the `logging` library (or an equivalent structured logger) with an appropriate level (`DEBUG`/`INFO`/`WARNING`/`ERROR`), never scattered `print()` calls throughout the code — `print()` has no level, no timestamp, and can't be selectively turned off.
- **Fail loud and early (fail-fast) when the state is invalid.** Prefer a clear exception at the moment the problem is detected over letting inconsistent data silently propagate to a later layer, where the original cause is no longer traceable.
- Use `with`/context managers for any resource that needs guaranteed cleanup (file, connection, lock) — this is also a form of error handling: it guarantees the resource is released even when an exception interrupts the block.

---

## 7. Python style conventions

Based on PEP 8 and PEP 20 ("Explicit is better than implicit"; "Readability counts") — applied when it makes sense, not as a blind rule.

| Element | Convention | Example |
|---|---|---|
| Variable/function/method | `snake_case` | `def calculate_total(...)` |
| Class/Exception | `PascalCase` | `class InvalidPayloadError` |
| Constant | `ALL_CAPS_WITH_UNDERSCORES` | `MAX_RETRIES = 3` |
| Protected/internal method | `_single_leading_underscore` | `def _parse_internal(self)` |
| Private method | `__double_leading_underscore` | `def __internal_only(self)` |

Other practices:

- Avoid one-letter names, except in very short blocks where the meaning is obvious from the immediate context (e.g., `for e in elements:`).
- Avoid redundant labeling: inside the `audio` module, prefer `audio.Core()` over `audio.AudioCore()` — the module name already gives the context.
- Prefer reverse notation for related variables: `elements`, `elements_active`, `elements_defunct` (not `active_elements`) — this groups them visually and helps autocomplete.
- Avoid comparing explicitly against `True`/`False`/`None`: prefer `if attr:`/`if not attr:`/`if attr is None:` over `if attr == True:`.
- Prefer list/dict comprehensions over manual loops when it doesn't sacrifice readability (KISS sets the limit — an unreadable, nested comprehension is worse than the explicit loop).
- Use `with` for any resource that needs guaranteed closing (files, connections) — see also Section 6.
- Import whole modules instead of individual symbols when it avoids circular imports and makes it clear where each name comes from (`import canteen` / `canteen.sessions.get_session()`, not `from canteen.sessions import get_session`), except when a third-party library's documentation explicitly recommends the direct-import pattern.
- Organize imports into three blocks, in this order, separated by a blank line: stdlib → third-party libraries → local project modules.
- Use type hints in function signatures (`def calculate(a: int, b: int) -> int:`) — it's executable documentation, verifiable with `mypy` where applicable, and doesn't replace Section 5.

---

## 8. Testing

- **FIRST:** tests should be **F**ast, **I**solated (no dependency on real network, database, or external service), **R**epeatable (same result every time), **S**elf-validating (the test itself reports pass/fail, no manual inspection), and **T**imely (written close to the code they test, not long after).
- **Arrange, Act, Assert:** organize each test into three clear parts — set up the necessary state, execute the action under test, verify the expected result.
- **Isolation from external I/O:** a test that depends on real network, a real database, or a real external API is inherently fragile (latency, availability, non-determinism) — prefer a test double (fake/stub) implementing the same contract, with known, frozen edge cases.
- Long, descriptive test names reduce the need for an explanatory docstring within the test itself.

---

## 9. Recommended tools (not mandatory — see Principle 0)

Mentioned here as a sensible default, not an unconditional requirement — adopt if the project benefits, skip if it's disproportionate overhead for the project's size:

- **`ruff`**: formatting + linting in a single binary (replaces the older `black`+`isort`+`flake8` combination).
- **`mypy`**: static type checking, leveraging the type hints from Section 7.
- **`pytest`**: testing framework.
- **`pre-commit`**: runs the tools above automatically before each commit.
