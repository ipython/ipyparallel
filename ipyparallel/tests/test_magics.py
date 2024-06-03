"""Test Parallel magics"""

import re
import signal
import sys
import time

import pytest
from IPython import get_ipython
from IPython.utils.io import capture_output

import ipyparallel as ipp
from ipyparallel import AsyncResult

from .clienttest import ClusterTestCase, generate_output, raises_remote


@pytest.mark.usefixtures('ipython_interactive')
class TestParallelMagics(ClusterTestCase):
    def test_px_blocking(self):
        ip = get_ipython()
        v = self.client[-1:]
        v.activate()
        v.block = True

        ip.run_line_magic('px', 'a=5')
        assert v['a'] == [5]
        ip.run_line_magic('px', 'a=10')
        assert v['a'] == [10]
        # just 'print a' works ~99% of the time, but this ensures that
        # the stdout message has arrived when the result is finished:
        with capture_output() as io:
            ip.run_line_magic(
                'px', 'import sys,time;print(a);sys.stdout.flush();time.sleep(0.2)'
            )
        assert '[stdout:' in io.stdout
        assert '\n\n' not in io.stdout
        assert io.stdout.rstrip().endswith('10')

    def test_px_var_expand(self):
        ip = get_ipython()
        v = self.client[-1:]
        v.activate()
        v.block = True
        ip.user_ns['x'] = 'client'
        v['x'] = 'engine'
        ip.run_line_magic('px', 'y=f"{x}"')
        assert v['y'] == ["engine"]

    def test_cell_px_var_expand(self):
        ip = get_ipython()
        v = self.client[-1:]
        v.activate()
        v.block = True
        ip.user_ns['x'] = 'client'
        v['x'] = 'engine'
        ip.user_ns["out_name"] = "theoutput"
        ip.run_cell_magic('px', '-o {out_name}', 'y=f"{x}"')
        assert v['y'] == ["engine"]
        assert "theoutput" in ip.user_ns
        assert isinstance(ip.user_ns["theoutput"], ipp.AsyncResult)

    def _check_generated_stderr(self, stderr, n):
        expected = [
            r'\[stderr:\d+\]',
            '^stderr$',
            '^stderr2$',
        ] * n

        assert '\n\n' not in stderr
        lines = stderr.splitlines()
        assert len(lines), len(expected) == stderr
        for line, expect in zip(lines, expected):
            if isinstance(expect, str):
                expect = [expect]
            for ex in expect:
                assert re.search(ex, line) is not None, f"Expected {ex!r} in {line!r}"

    def _check_expected_lines_unordered(self, expected, lines):
        for expect in expected:
            found = False
            for line in lines:
                found = found | (re.search(expect, line) is not None)
                if found:
                    break
            assert found, f"Expected {expect!r} in output: {lines}"

    def test_cellpx_block_args(self):
        """%%px --[no]block flags work"""
        ip = get_ipython()
        v = self.client[-1:]
        v.activate()
        v.block = False

        for block in (True, False):
            v.block = block
            ip.run_line_magic("pxconfig", "--verbose")
            with capture_output(display=False) as io:
                ip.run_cell_magic("px", "", "1")
            if block:
                assert io.stdout.startswith("Parallel"), io.stdout
            else:
                assert io.stdout.startswith("Async"), io.stdout

            with capture_output(display=False) as io:
                ip.run_cell_magic("px", "--block", "1")
            assert io.stdout.startswith("Parallel"), io.stdout

            with capture_output(display=False) as io:
                ip.run_cell_magic("px", "--noblock", "1")
            assert io.stdout.startswith("Async"), io.stdout

    def test_cellpx_groupby_engine(self):
        """%%px --group-outputs=engine"""
        ip = get_ipython()
        v = self.client[:]
        v.block = True
        v.activate()

        v['generate_output'] = generate_output

        with capture_output(display=False) as io:
            ip.run_cell_magic(
                'px', '--group-outputs=engine --no-stream', 'generate_output()'
            )

        assert '\n\n' not in io.stdout
        lines = io.stdout.splitlines()
        expected = [
            r'\[stdout:\d+\]',
            'stdout',
            'stdout2',
            r'\[output:\d+\]',
            r'IPython\.core\.display\.HTML',
            r'IPython\.core\.display\.Math',
            r'Out\[\d+:\d+\]:.*IPython\.core\.display\.Math',
        ] * len(v)

        assert len(lines), len(expected) == io.stdout
        for line, expect in zip(lines, expected):
            if isinstance(expect, str):
                expect = [expect]
            for ex in expect:
                assert re.search(ex, line) is not None, f"Expected {ex!r} in {line!r}"

        self._check_generated_stderr(io.stderr, len(v))

    def test_cellpx_groupby_order(self):
        """%%px --group-outputs=order"""
        ip = get_ipython()
        v = self.client[:]
        v.block = True
        v.activate()

        v['generate_output'] = generate_output

        with capture_output(display=False) as io:
            ip.run_cell_magic(
                'px', '--group-outputs=order --no-stream', 'generate_output()'
            )

        assert '\n\n' not in io.stdout
        lines = io.stdout.splitlines()
        expected = []
        expected.extend(
            [
                r'\[stdout:\d+\]',
                'stdout',
                'stdout2',
            ]
            * len(v)
        )
        expected.extend(
            [
                r'\[output:\d+\]',
                'IPython.core.display.HTML',
            ]
            * len(v)
        )
        expected.extend(
            [
                r'\[output:\d+\]',
                'IPython.core.display.Math',
            ]
            * len(v)
        )
        expected.extend([r'Out\[\d+:\d+\]:.*IPython\.core\.display\.Math'] * len(v))

        assert len(lines), len(expected) == io.stdout
        for line, expect in zip(lines, expected):
            if isinstance(expect, str):
                expect = [expect]
            for ex in expect:
                assert re.search(ex, line) is not None, f"Expected {ex!r} in {line!r}"

        self._check_generated_stderr(io.stderr, len(v))

    def test_cellpx_groupby_type(self):
        """%%px --group-outputs=type"""
        ip = get_ipython()
        v = self.client[:]
        v.block = True
        v.activate()

        v['generate_output'] = generate_output

        with capture_output(display=False) as io:
            ip.run_cell_magic(
                'px', '--group-outputs=type --no-stream', 'generate_output()'
            )

        assert '\n\n' not in io.stdout
        lines = io.stdout.splitlines()

        expected = []
        expected.extend(
            [
                r'\[stdout:\d+\]',
                'stdout',
                'stdout2',
            ]
            * len(v)
        )
        expected.extend(
            [
                r'\[output:\d+\]',
                r'IPython\.core\.display\.HTML',
                r'IPython\.core\.display\.Math',
            ]
            * len(v)
        )
        expected.extend([(r'Out\[\d+:\d+\]', r'IPython\.core\.display\.Math')] * len(v))

        assert len(lines), len(expected) == io.stdout
        for line, expect in zip(lines, expected):
            if isinstance(expect, str):
                expect = [expect]
            for ex in expect:
                assert re.search(ex, line) is not None, f"Expected {ex!r} in {line!r}"

        self._check_generated_stderr(io.stderr, len(v))

    def test_cellpx_error_stream(self):
        self.minimum_engines(6)
        ip = get_ipython()
        v = self.client[:]
        v.block = True
        v.activate()
        v.scatter("rank", range(len(v)), flatten=True, block=True)

        with capture_output(display=False) as io:
            with pytest.raises(ipp.error.AlreadyDisplayedError) as exc_info:
                ip.run_cell_magic(
                    "px",
                    "--stream",
                    "import time; time.sleep(rank); raise RuntimeError(f'oops! {rank}')",
                )

        print(io.stdout)
        print(io.stderr, file=sys.stderr)
        assert 'RuntimeError' in io.stderr
        assert len(v) <= io.stderr.count("RuntimeError:") < len(v) * 2
        printed_tb = "\n".join(exc_info.value.render_traceback())
        assert printed_tb == f"{len(v)} errors"

    def test_cellpx_error_no_stream(self):
        self.minimum_engines(6)
        ip = get_ipython()
        v = self.client[:]
        v.block = True
        v.activate()
        v.scatter("rank", range(len(v)), flatten=True, block=True)

        with capture_output(display=False) as io:
            with pytest.raises(ipp.error.CompositeError) as exc_info:
                ip.run_cell_magic(
                    "px",
                    "--no-stream",
                    "import time; time.sleep(rank); raise RuntimeError(f'oops! {rank}')",
                )

        print(io.stdout)
        print(io.stderr, file=sys.stderr)
        assert 'RuntimeError' not in io.stderr
        assert io.stderr.strip() == ""
        printed_tb = "\n".join(exc_info.value.render_traceback())
        assert printed_tb.count("RuntimeError:") >= ipp.error.CompositeError.tb_limit

    def test_cellpx_stream(self):
        """%%px --stream"""
        self.minimum_engines(6)
        ip = get_ipython()
        v = self.client[:]
        v.block = True
        v.activate()

        v['generate_output'] = generate_output

        with capture_output(display=False) as io:
            ip.run_cell_magic('px', '--stream', 'generate_output()')

        print(io.stdout)
        print(io.stderr, file=sys.stderr)
        assert '\n\n' not in io.stdout
        print(io.stdout)
        lines = io.stdout.splitlines()
        expected = []
        expected.extend(
            [
                r'\[stdout:\d+\]',
                'stdout',
                r'\[stdout:\d+\]',
                'stdout2',
                r'\[output:\d+\]',
                r'IPython\.core\.display\.HTML',
                r'\[output:\d+\]',
                r'IPython\.core\.display\.Math',
                r'Out\[\d+:\d+\]:.*IPython\.core\.display\.Math',
            ]
            * len(v)
        )

        # Check that all expected lines are in the output
        self._check_expected_lines_unordered(expected, lines)

        assert (
            len(expected) - len(v) <= len(lines) <= len(expected)
        ), f"expected {len(expected)} lines, got: {io.stdout}"

        # Do the same for stderr
        print(io.stderr, file=sys.stderr)
        assert '\n\n' not in io.stderr
        lines = io.stderr.splitlines()
        expected = []
        expected.extend(
            [
                r'\[stderr:\d+\]',
                'stderr',
                r'\[stderr:\d+\]',
                'stderr2',
            ]
            * len(v)
        )
        self._check_expected_lines_unordered(expected, lines)
        assert (
            len(expected) - len(v) <= len(lines) <= len(expected)
        ), f"expected {len(expected)} lines, got: {io.stderr}"

    def test_px_nonblocking(self):
        ip = get_ipython()
        v = self.client[-1:]
        v.activate()
        v.block = False

        ip.run_line_magic('px', 'a=5')
        assert v['a'] == [5]
        ip.run_line_magic('px', 'a=10')
        assert v['a'] == [10]
        ip.run_line_magic('pxconfig', '--verbose')
        with capture_output() as io:
            ar = ip.run_line_magic('px', 'print (a)')
        assert isinstance(ar, AsyncResult)
        assert 'Async' in io.stdout
        assert '[stdout:' not in io.stdout
        assert '\n\n' not in io.stdout

        ar = ip.run_line_magic('px', '1/0')
        with raises_remote(ZeroDivisionError):
            ar.get()

    def test_autopx_blocking(self):
        ip = get_ipython()
        v = self.client[-1]
        v.activate()
        v.block = True

        with capture_output(display=False) as io:
            ip.run_line_magic('autopx', '')
            ip.run_cell('\n'.join(('a=5', 'b=12345', 'c=0')))
            ip.run_cell('b*=2')
            ip.run_cell('print (b)')
            ip.run_cell('b')
            ip.run_cell("b/c")
            ip.run_line_magic('autopx', '')

        output = io.stdout

        assert output.startswith('%autopx enabled'), output
        assert output.rstrip().endswith('%autopx disabled'), output
        assert 'ZeroDivisionError' in output
        assert '\nOut[' in output
        assert ': 24690' in output
        ar = v.get_result(-1)
        # prevent TaskAborted on pulls, due to ZeroDivisionError
        time.sleep(0.5)
        assert v['a'] == 5
        assert v['b'] == 24690
        with raises_remote(ZeroDivisionError):
            ar.get()

    def test_autopx_nonblocking(self):
        ip = get_ipython()
        v = self.client[-1]
        v.activate()
        v.block = False

        with capture_output() as io:
            ip.run_line_magic('autopx', '')
            ip.run_cell('\n'.join(('a=5', 'b=10', 'c=0')))
            ip.run_cell('print (b)')
            ip.run_cell('import time; time.sleep(0.1)')
            ip.run_cell("b/c")
            ip.run_cell('b*=2')
            ip.run_line_magic('autopx', '')

        output = io.stdout.rstrip()
        assert output.startswith('%autopx enabled'), output
        assert output.endswith('%autopx disabled'), output
        assert 'ZeroDivisionError' not in output
        ar = v.get_result(-2, owner=False)
        with raises_remote(ZeroDivisionError):
            ar.get()
        # prevent TaskAborted on pulls, due to ZeroDivisionError
        time.sleep(0.5)
        assert v['a'] == 5
        # b*=2 will not fire, due to abort
        assert v['b'] == 10

    def test_result(self):
        ip = get_ipython()
        v = self.client[-1]
        v.activate()
        data = dict(a=111, b=222)
        v.push(data, block=True)

        for name in ('a', 'b'):
            ip.run_line_magic('px', name)
            with capture_output(display=False) as io:
                ip.run_line_magic('pxresult', '')
            assert str(data[name]) in io.stdout

    def test_px_pylab(self):
        """%pylab works on engines"""
        pytest.importorskip('matplotlib')
        ip = get_ipython()
        v = self.client[-1]
        v.block = True
        v.activate()

        with capture_output() as io:
            ip.run_line_magic("px", "%pylab inline")

        assert (
            "Populating the interactive namespace from numpy and matplotlib"
            in io.stdout
        )

        with capture_output(display=False) as io:
            ip.run_line_magic("px", "plot(rand(100))")
        assert 'Out[' in io.stdout
        assert 'matplotlib.lines' in io.stdout

    def test_pxconfig(self):
        ip = get_ipython()
        rc = self.client
        v = rc.activate(-1, '_tst')
        assert v.targets == rc.ids[-1]
        ip.run_line_magic("pxconfig_tst", "-t :")
        assert v.targets == rc.ids
        ip.run_line_magic("pxconfig_tst", "-t ::2")
        assert v.targets == rc.ids[::2]
        ip.run_line_magic("pxconfig_tst", "-t 1::2")
        assert v.targets == rc.ids[1::2]
        ip.run_line_magic("pxconfig_tst", "-t 1")
        assert v.targets == 1
        ip.run_line_magic("pxconfig_tst", "--block")
        assert v.block is True
        ip.run_line_magic("pxconfig_tst", "--noblock")
        assert v.block is False

    def test_cellpx_targets(self):
        """%%px --targets doesn't change defaults"""
        ip = get_ipython()
        rc = self.client
        view = rc.activate(rc.ids)
        assert view.targets == rc.ids
        ip.run_line_magic('pxconfig', '--verbose')
        for cell in ("pass", "1/0"):
            with capture_output(display=False) as io:
                try:
                    ip.run_cell_magic("px", "--targets all", cell)
                except ipp.RemoteError:
                    pass
            assert 'engine(s): all' in io.stdout
            assert view.targets == rc.ids

    def test_cellpx_block(self):
        """%%px --block doesn't change default"""
        ip = get_ipython()
        rc = self.client
        view = rc.activate(rc.ids)
        view.block = False
        assert view.targets == rc.ids
        ip.run_line_magic('pxconfig', '--verbose')
        for cell in ("pass", "1/0"):
            with capture_output(display=False) as io:
                try:
                    ip.run_cell_magic("px", "--block", cell)
                except ipp.RemoteError:
                    pass
            assert 'Async' not in io.stdout
            assert view.block is False

    def cellpx_keyboard_interrupt_test_helper(self, sig=None):
        """%%px with Keyboard Interrupt on blocking execution"""
        ip = get_ipython()
        v = self.client[:]
        v.block = True
        v.activate()

        def _sigalarm(sig, frame):
            raise KeyboardInterrupt

        signal.signal(signal.SIGALRM, _sigalarm)
        signal.alarm(2)
        with capture_output(display=False) as io:
            ip.run_cell_magic(
                "px",
                "" if sig is None else f"--signal-on-interrupt {sig}",
                "print('Entering...'); import time; time.sleep(5); print('Exiting...');",
            )

        print(io.stdout)
        print(io.stderr, file=sys.stderr)
        assert (
            'Received Keyboard Interrupt. Sending signal {} to engines...'.format(
                "SIGINT" if sig is None else sig
            )
            in io.stderr
        )
        assert 'Exiting...' not in io.stdout

    @pytest.mark.skipif(
        sys.platform.startswith("win"), reason="Signal tests don't pass on Windows yet"
    )
    def test_cellpx_keyboard_interrupt_default(self):
        self.cellpx_keyboard_interrupt_test_helper()

    @pytest.mark.skipif(
        sys.platform.startswith("win"), reason="Signal tests don't pass on Windows yet"
    )
    def test_cellpx_keyboard_interrupt_SIGINT(self):
        self.cellpx_keyboard_interrupt_test_helper("SIGINT")

    @pytest.mark.skipif(
        sys.platform.startswith("win"), reason="Signal tests don't pass on Windows yet"
    )
    def test_cellpx_keyboard_interrupt_signal_2(self):
        self.cellpx_keyboard_interrupt_test_helper("2")

    @pytest.mark.skipif(
        sys.platform.startswith("win"), reason="Signal tests don't pass on Windows yet"
    )
    def test_cellpx_keyboard_interrupt_signal_0(self):
        self.cellpx_keyboard_interrupt_test_helper("0")

    @pytest.mark.skipif(
        sys.platform.startswith("win"), reason="Signal tests don't pass on Windows yet"
    )
    def test_cellpx_keyboard_interrupt_SIGKILL(self):
        self.cellpx_keyboard_interrupt_test_helper("SIGKILL")

    @pytest.mark.skipif(
        sys.platform.startswith("win"), reason="Signal tests don't pass on Windows yet"
    )
    def test_cellpx_keyboard_interrupt_signal_9(self):
        self.cellpx_keyboard_interrupt_test_helper("9")
