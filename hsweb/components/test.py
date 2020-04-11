from .. import components as c
from dataclasses import dataclass


@dataclass
class C1(c.Component):
    val: str

    def tree(self) -> c.Nodes:
        z: c.Nodes = []
        z += c.Tag("p", content=c.Text("hello"))
        z += C2(self.val)
        return z


@dataclass
class C2(c.Component):
    val: str

    def tree(self) -> c.Nodes:
        z: c.Nodes = []
        with c.Tag("div", {"class": "y"}).into(z):
            z += c.Text(self.val)
        return z


def test_components() -> None:
    html = C1("x").render()
    assert '<p>hello</p><div class="y">x</div>' == html
