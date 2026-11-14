"""Teshis: MetaTrader5, bir yardimci surecin kosullarinda (stdin/stdout boru, penceresiz, stdin'i okuyan is parcacigi)
neden yanit vermiyor? Hicbir emir gondermez, giris bilgisi vermez; yalnizca terminale baglanip ayrilir."""
import os, subprocess, sys, threading, time

TERMINAL = r"C:\Program Files\Tickmill Europe MT5 Terminal\terminal64.exe"
LIMIT = 40


def child(variant: str) -> None:
    import faulthandler
    faulthandler.dump_traceback_later(25, exit=True, file=sys.stderr)   # takilirsa nerede takildigini yazar
    t0 = time.time()

    def say(what: str) -> None:
        print(f"ADIM {what} t={time.time() - t0:.1f}", file=sys.stderr, flush=True)

    def reader() -> None:
        stream = os.fdopen(os.dup(0), "rb", buffering=0)
        say("okuyucu-basladi")
        stream.read(4)                       # botun yardimci sureci gibi: stdin'de bekler
        say("okuyucu-dondu")

    def start_reader() -> None:
        threading.Thread(target=reader, daemon=True).start()
        time.sleep(1.0)

    def init(mt5) -> None:
        ok = mt5.initialize(TERMINAL, timeout=15000)
        say(f"initialize={ok} hata={mt5.last_error()}")
        if ok:
            mt5.shutdown()

    if variant in ("B", "D"):
        start_reader()
    import MetaTrader5 as mt5
    say("import-bitti")
    if variant == "C":
        start_reader()
    if variant in ("B", "C"):
        from concurrent.futures import ThreadPoolExecutor
        ThreadPoolExecutor(max_workers=1).submit(init, mt5).result()
    else:
        init(mt5)
    say("BITTI")
    faulthandler.cancel_dump_traceback_later()
    os._exit(0)


def driver() -> None:
    flags = 0x08000000 if sys.platform == "win32" else 0          # CREATE_NO_WINDOW
    names = {"A": "okuyucu yok (kontrol)", "B": "okuyucu, sonra import, initialize is parcaciginda (bugunku yardimci)",
             "C": "once import, sonra okuyucu, initialize is parcaciginda", "D": "okuyucu, sonra import, initialize ana parcacikta"}
    for variant in "ABCD":
        args = [sys.executable] + (["-I"] if os.environ.get("TESHIS_I", "1") == "1" else []) + [os.path.abspath(__file__), variant]
        p = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, creationflags=flags)
        lines: list[str] = []
        t = threading.Thread(target=lambda: lines.extend(x.decode("utf-8", "replace").rstrip() for x in p.stderr), daemon=True)
        t.start()
        try:
            code = p.wait(timeout=LIMIT)
            state = f"bitti (kod {code})"
        except subprocess.TimeoutExpired:
            p.kill()
            p.wait()
            state = f"{LIMIT} sn'de BITMEDI, sonlandirildi"
        t.join(3)
        print(f"=== {variant}: {names[variant]} -> {state}")
        for line in lines[-25:]:
            print("   ", line)
        with_stdin = p.stdin
        try:
            with_stdin.close()
        except OSError:
            pass


if __name__ == "__main__":
    child(sys.argv[1]) if len(sys.argv) > 1 else driver()
