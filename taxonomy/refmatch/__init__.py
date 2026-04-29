"""Reusable reference parsing and matching helpers.

The matching pipeline is designed to link every parsed reference to at least one
durable external identifier where possible. A taxonomy ``Article`` match is one
useful route, but not the only goal: DOI, BatLit, and BHL links are equally
valuable when they provide a stable way to identify the same bibliographic work.
"""
