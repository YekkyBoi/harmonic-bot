"""Teshis 2: stdin'de bekleyen is parcacigi varken hangi yuklemeler takiliyor, ve beklemeden (yoklayarak) okuyan
is parcacigi bunu cozuyor mu? Hicbir emir gondermez, giris bilgisi vermez; yalnizca terminale baglanip ayrilir."""
import os, subprocess, sys, threading, time

TERMINAL = r"C:\Program Files\Tickmill Europe MT5 Terminal\terminal64.exe"
LIMIT = 40


def wait_readable(fd: int) -> bool:
    """Veri gelene kadar YOKLAYARAK bekler (stdin'de bekleyen bir okuma birakmaz). False: boru kapandi."""
    if sys.platform != "win32":
        import select
        select.select([fd], [], [])
        return True
    import ctypes, msvcrt
    from ctypes import wintypes
    k32 = ctypes.WinDLL("kernel32", use_last_error=True)
    k32.PeekNamedPipe.argtypes = [wintypes.HANDLE, ctypes.c_void_p, wintypes.DWORD, ctypes.c_void_p,
                                  ctypes.POINTER(wintypes.DWORD), ctypes.c_void_p]
    k32.PeekNamedPipe.restype = wintypes.BOOL
    handle = msvcrt.get_osfhandle(fd)
    avail = wintypes.DWORD(0)
    while True:
        if not k32.PeekNamedPipe(handle, None, 0, None, ctypes.byref(avail), None):
            return False
        if avail.value:
            return True
        time.sleep(0.01)


def child(variant: str) -> None:
    import faulthandler
    faulthandler.dump_traceback_later(25, exit=True, file=sys.stderr)
    t0 = time.time()
    got = threading.Event()

    def say(what: str) -> None:
        print(f"ADIM {what} t={time.time() - t0:.1f}", file=sys.stderr, flush=True)

    def reader(poll: bool) -> None:
        fd = os.dup(0)
        say("okuyucu-basladi" + (" (yoklayarak)" if poll else " (bekleyerek)"))
        if poll and not wait_readable(fd):
            say("okuyucu: boru kapandi")
            return
        data = os.read(fd, 4)
        say(f"okuyucu-dondu {data!r}")
        got.set()

    def start_reader(poll: bool = False) -> None:
        threading.Thread(target=reader, args=(poll,), daemon=True).start()
        time.sleep(1.0)

    def init(mt5) -> None:
        ok = mt5.initialize(TERMINAL, timeout=15000)
        say(f"initialize={ok} hata={mt5.last_error()}")
        if ok:
            mt5.shutdown()

    def in_thread(fn, *args) -> None:
        from concurrent.futures import ThreadPoolExecutor
        ThreadPoolExecutor(max_workers=1).submit(fn, *args).result()

    def load(names: str) -> None:
        import importlib
        for name in names.split():
            t = time.time()
            try:
                importlib.import_module(name)
                say(f"yuklendi {name} ({time.time() - t:.2f} sn)")
            except Exception as exc:  # noqa: BLE001
                say(f"yuklenemedi {name}: {type(exc).__name__}")

    if variant == "E":                       # yoklayan okuyucu ONCE, sonra import + initialize + veri alimi
        start_reader(poll=True)
        import MetaTrader5 as mt5
        say("import-bitti")
        in_thread(init, mt5)
        got.wait(8)
        say("veri-geldi" if got.is_set() else "veri GELMEDI")
    elif variant == "F":                     # once import, bekleyen okuyucu, sonra BASKA yuklemeler
        import MetaTrader5 as mt5
        say("import-bitti")
        start_reader()
        in_thread(init, mt5)
        load("sqlite3 decimal unicodedata ssl ctypes lzma bz2 numpy.random numpy.linalg numpy.fft")
    elif variant == "G":                     # bekleyen okuyucu, sonra yalnizca numpy
        start_reader()
        load("numpy")
    elif variant == "H":                     # bekleyen okuyucu, sonra Python'un kendi eklentileri
        start_reader()
        load("sqlite3 decimal unicodedata ssl ctypes lzma bz2")
    elif variant == "I":                     # bekleyen okuyucu, sonra MetaTrader5'in cekirdegi (numpy onceden yuklu)
        import numpy  # noqa: F401
        say("numpy-yuklendi")
        start_reader()
        load("MetaTrader5")
    say("BITTI")
    faulthandler.cancel_dump_traceback_later()
    os._exit(0)


def driver() -> None:
    flags = 0x08000000 if sys.platform == "win32" else 0          # CREATE_NO_WINDOW
    names = {"E": "yoklayan okuyucu, sonra import + initialize + veri", "F": "once import, bekleyen okuyucu, sonra baska yuklemeler",
             "G": "bekleyen okuyucu, sonra numpy", "H": "bekleyen okuyucu, sonra Python eklentileri",
             "I": "numpy, bekleyen okuyucu, sonra MetaTrader5"}
    for variant in "EFGHI":
        args = [sys.executable] + (["-I"] if os.environ.get("TESHIS_I", "1") == "1" else []) + [os.path.abspath(__file__), variant]
        p = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, creationflags=flags)
        lines: list[str] = []
        t = threading.Thread(target=lambda: lines.extend(x.decode("utf-8", "replace").rstrip() for x in p.stderr), daemon=True)
        t.start()
        if variant == "E":
            time.sleep(5)
            try:
                p.stdin.write(b"ping")
                p.stdin.flush()
            except OSError:
                pass
        try:
            code = p.wait(timeout=LIMIT)
            state = f"bitti (kod {code})"
        except subprocess.TimeoutExpired:
            p.kill()
            p.wait()
            state = f"{LIMIT} sn'de BITMEDI, sonlandirildi"
        t.join(3)
        print(f"=== {variant}: {names[variant]} -> {state}")
        steps = [x for x in lines if x.startswith("ADIM")]
        trace = [x for x in lines if not x.startswith("ADIM")]
        for line in steps:
            print("   ", line)
        for line in trace[:3] + (["    ..."] if len(trace) > 12 else []) + trace[3:][-9:]:
            print("   ", line)
        try:
            p.stdin.close()
        except OSError:
            pass


if __name__ == "__main__":
    child(sys.argv[1]) if len(sys.argv) > 1 else driver()
