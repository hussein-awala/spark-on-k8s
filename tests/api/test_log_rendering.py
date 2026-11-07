from __future__ import annotations

import re
import shutil
import subprocess
from importlib.resources import files

import pytest


def test_driver_logs_are_rendered_as_literal_text():
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node.js is required to exercise the browser log-rendering callback")
    template = files("spark_on_k8s").joinpath("api/webserver/templates/app_logs.html").read_text()
    browser_script = re.search(r"<script>(.*?)</script>", template, re.DOTALL).group(1)
    # Run the actual browser callback against a minimal DOM. HTML access fails
    # deliberately; text and append operations have normal DOM semantics.
    harness = r"""
const assert = require('node:assert/strict');
const vm = require('node:vm');
const fs = require('node:fs');

class Element {
    constructor(tagName) {
        this.tagName = tagName;
        this.children = [];
        this._textContent = '';
        this.scrollTop = 0;
    }
    set textContent(value) {
        this._textContent = String(value);
        this.children = [];
    }
    get textContent() {
        return this._textContent + this.children.map(child => child.textContent).join('');
    }
    appendChild(child) { this.children.push(child); }
    get scrollHeight() { return this.children.length; }
    get innerHTML() { throw new Error('Log messages must not be interpreted as HTML'); }
    set innerHTML(value) { throw new Error('Log messages must not be interpreted as HTML'); }
}

const logs = new Element('div');
let socket;
class WebSocket {
    constructor(url) { this.url = url; socket = this; }
}
vm.runInNewContext(fs.readFileSync(0, 'utf8'), {
    document: {
        getElementById: id => { assert.equal(id, 'logs-container'); return logs; },
        createElement: tag => new Element(tag),
    },
    window: { location: { protocol: 'https:', hostname: 'example.test', port: '' } },
    WebSocket,
    console,
});

const messages = [
    '<img src=x onerror="window.unexpectedExecution=true">',
    'value < threshold && count > 0\n  indented log line',
];
for (const message of messages) socket.onmessage({ data: message });
assert.equal(logs.children.length, messages.length);
assert.deepEqual(logs.children.map(child => child.textContent), messages);
assert.ok(logs.children.every(child => child.tagName === 'p'));
assert.equal(logs.scrollTop, logs.scrollHeight);
"""

    result = subprocess.run([node, "-e", harness], input=browser_script, capture_output=True, text=True)

    assert result.returncode == 0, result.stderr
