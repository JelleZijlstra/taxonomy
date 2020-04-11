import asyncio
from contextlib import contextmanager
from dataclasses import dataclass, field
import html
import re
from typing import ClassVar, Dict, Iterable, Iterator, Type
from typing_extensions import Protocol


class InvalidTag(Exception):
    pass


class Node:
    async def arender(self) -> str:
        return self.render()

    def render(self) -> str:
        return ""

    def __iter__(self) -> Iterator["Node"]:
        yield self


Nodes = Iterable[Node]


@dataclass
class Text(Node):
    text: str

    def render(self) -> str:
        return html.escape(self.text)


@dataclass
class Tag(Node):
    tag_name: str
    attrs: Dict[str, str] = field(default_factory=dict)
    content: Nodes = field(default_factory=list)

    def __post_init__(self) -> None:
        _assert_valid_tag(self.tag_name)
        for key in self.attrs:
            _assert_valid_tag(key)

    async def arender(self) -> str:
        rendered = await asyncio.gather(*[node.arender() for node in self.content])
        inner = "".join(rendered)
        return f"{self.open_tag()}{inner}</{self.tag_name}>"

    def render(self) -> str:
        inner = "".join(node.render() for node in self.content)
        return f"{self.open_tag()}{inner}</{self.tag_name}>"

    def open_tag(self) -> str:
        attrs = " ".join(
            f'{key}="{html.escape(value)}"' for key, value in self.attrs.items()
        )
        attrs = f" {attrs}" if attrs else ""
        return f"<{self.tag_name}{attrs}>"

    @contextmanager
    def into(self, z: Nodes) -> Iterator[None]:
        z += _EnterTag(self)
        yield
        z += _LeaveTag(self)


@dataclass
class _EnterTag(Node):
    tag: Tag

    def render(self) -> str:
        return self.tag.open_tag()


@dataclass
class _LeaveTag(Node):
    tag: Tag

    def render(self) -> str:
        return f"</{self.tag.tag_name}>"


class SelfClosingTag(Node):
    tag_name: str
    attrs: Dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _assert_valid_tag(self.tag_name)
        for key in self.attrs:
            _assert_valid_tag(key)

    def render(self) -> str:
        attrs = " ".join(
            f'{key}="{html.escape(value)}"' for key, value in self.attrs.items()
        )
        attrs = f" {attrs}" if attrs else ""
        return f"<{self.tag_name}{attrs}/>"


class _TagCallable(Protocol):
    def __call__(self, content: Nodes = [], **attrs: str) -> Tag:
        pass


_ATTR_MAPPING = {"class_": "class"}

def _make_tag(tag_name: str) -> _TagCallable:
    def inner(content: Nodes = [], **attrs: str) -> Tag:
        attrs = {_ATTR_MAPPING.get(k, k): v for k, v in attrs.items()}
        return Tag(tag_name, attrs, content)

    return inner


Div = _make_tag("div")
Span = _make_tag("span")
OL = _make_tag("ol")
UL = _make_tag("ul")
LI = _make_tag("li")
P = _make_tag("p")
B = _make_tag("b")
I = _make_tag("i")
A = _make_tag("a")
Table = _make_tag("table")
TR = _make_tag("tr")
TH = _make_tag("th")
TD = _make_tag("td")
H1 = _make_tag("h1")
H2 = _make_tag("h2")
H3 = _make_tag("h3")
H4 = _make_tag("h4")
H5 = _make_tag("h5")


class Component(Node):
    wrapper_tag: ClassVar[Type[Tag]] = Div

    async def atree(self) -> Nodes:
        return self.tree()

    def tree(self) -> Nodes:
        return []

    async def arender(self) -> str:
        z: Nodes = []
        with self.wrapper_tag().into(z):
            z += await self.atree()
        rendered = await asyncio.gather(*[node.arender() for node in z])
        return "".join(rendered)

    def render(self) -> str:
        return "".join(node.render() for node in self.tree())


def _assert_valid_tag(tag: str) -> None:
    if not re.match(r"^[a-z][a-z\d_\-]*$", tag):
        raise InvalidTag(tag)
