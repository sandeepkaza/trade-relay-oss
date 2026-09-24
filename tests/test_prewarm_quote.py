"""test_prewarm_quote.py — IBKR cold-quote pre-warm (flag-gated, BUY-only entry
optimisation in ibkr_sdk_bridge.prewarm_quote + the ibkr_broker flag).

Pure guard-logic tests: no real IB connection, no network. We patch the module
seams (_DRY_RUN, _get_ib) and assert prewarm_quote short-circuits without ever
reaching the broker on the paths that must stay cheap/no-op.
"""
import asyncio
import unittest

import app.execution.ibkr_sdk_bridge as bridge
import app.execution.ibkr_broker as broker


async def _boom(*_a, **_k):
    raise AssertionError("_get_ib must not be called on this path")


class PrewarmQuote(unittest.TestCase):
    def setUp(self):
        self._dry = bridge._DRY_RUN
        self._get_ib = bridge._get_ib
        self._cache_snapshot = list(bridge._TICKER_CACHE.items())

    def tearDown(self):
        bridge._DRY_RUN = self._dry
        bridge._get_ib = self._get_ib
        bridge._TICKER_CACHE.clear()
        bridge._TICKER_CACHE.update(self._cache_snapshot)

    def test_dry_run_is_noop(self):
        """In dry_run there's no live broker — must return without touching IB."""
        bridge._DRY_RUN = lambda: True
        bridge._get_ib = _boom
        asyncio.run(bridge.prewarm_quote("SPXW260612C07420000"))  # no raise == pass

    def test_empty_osi_is_noop(self):
        bridge._DRY_RUN = lambda: False
        bridge._get_ib = _boom
        asyncio.run(bridge.prewarm_quote(""))

    def test_warm_cache_short_circuits(self):
        """Already-subscribed symbol: re-warm must NOT re-qualify/re-subscribe."""
        bridge._DRY_RUN = lambda: False
        bridge._get_ib = _boom
        osi = "SPXW260612C07420000"
        bridge._TICKER_CACHE[osi] = ("contract", "ticker")  # pretend already streaming
        asyncio.run(bridge.prewarm_quote(osi))               # must short-circuit
        self.assertIn(osi, bridge._TICKER_CACHE)

    def test_errors_are_swallowed(self):
        """Best-effort: a broker error in prewarm must never propagate (it can't
        be allowed to fail the order path that fired it)."""
        bridge._DRY_RUN = lambda: False

        async def _raise(*_a, **_k):
            raise RuntimeError("ib down")

        bridge._get_ib = _raise
        # uncached symbol → reaches _get_ib → raises internally → swallowed
        asyncio.run(bridge.prewarm_quote("SPXW260612C07999000"))  # no raise == pass

    def test_flag_defaults_off(self):
        """No [ibkr] prewarm_quote_enabled in config → inert by default."""
        self.assertFalse(broker._PREWARM_QUOTE_ENABLED())


class PrewarmSymbolDispatch(unittest.TestCase):
    """broker_router.prewarm_symbol — the on-mention warmer's gated dispatcher."""

    def setUp(self):
        import app.execution.broker_router as br
        import app.execution.ibkr_sdk_bridge as bridge
        self.br = br
        self.bridge = bridge
        self._ab = br._active_broker
        self._pq = bridge.prewarm_quote
        self._gb = br.cfg.getboolean

    def tearDown(self):
        self.br._active_broker = self._ab
        self.bridge.prewarm_quote = self._pq
        self.br.cfg.getboolean = self._gb

    def _run(self, osi, broker_name, flag):
        """Drive prewarm_symbol under a running loop; return list of OSIs that
        actually reached prewarm_quote."""
        called = []

        async def fake_pq(o):
            called.append(o)

        self.bridge.prewarm_quote = fake_pq
        self.br._active_broker = lambda: broker_name
        self.br.cfg.getboolean = (
            lambda s, k, fallback=False: flag
            if (s, k) == ("ibkr", "prewarm_quote_enabled")
            else self._gb(s, k, fallback)
        )

        async def run():
            self.br.prewarm_symbol(osi)
            await asyncio.sleep(0)   # let the fire-and-forget task run

        asyncio.run(run())
        return called

    def test_public_broker_noop(self):
        self.assertEqual(self._run("SPXW260612C07420000", "public", True), [])

    def test_ibkr_flag_off_noop(self):
        self.assertEqual(self._run("SPXW260612C07420000", "ibkr", False), [])

    def test_ibkr_flag_on_fires(self):
        self.assertEqual(self._run("SPXW260612C07420000", "ibkr", True), ["SPXW260612C07420000"])

    def test_empty_osi_noop(self):
        self.assertEqual(self._run("", "ibkr", True), [])


if __name__ == "__main__":
    unittest.main()
