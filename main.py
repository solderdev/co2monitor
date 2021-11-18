"""
. create udev rules /etc/udev/rules.d/90-co2tfa.rules
    # ACTION=="add",SUBSYSTEMS=="usb", KERNEL=="hidraw*", ATTRS{idVendor}=="04d9", ATTRS{idProduct}=="a052", GROUP="users", MODE="0660", SYMLINK+="co2mon"
    (use both lines) https://github.com/trezor/cython-hidapi/issues/47
    KERNEL=="hidraw*", ATTRS{idVendor}=="04d9", ATTRS{idProduct}=="a052", GROUP="plugdev", MODE="0666"
    SUBSYSTEM=="usb", ATTRS{idVendor}=="04d9", ATTRS{idProduct}=="a052", GROUP="plugdev", MODE="0666"
. sudo apt install libhidapi-dev
. [sudo] pip3 install hidapi
. ([sudo] pip3 uninstall hid)
"""

# https://github.com/vfilimonov/co2meter/tree/master/co2meter
# http://co2meters.com/Documentation/AppNotes/AN146-RAD-0401-serial-communication.pdf
# https://github.com/KristofRobot/openhab-config/blob/master/co2mon/co2mon.py
# https://github.com/ishaanv/holtek-co2-logger/blob/master/co2_logger.py
# https://github.com/MathieuSchopfer/tfa-airco2ntrol-mini

import sys
import time
import hid
import urllib.request as requests
import urllib.error

import metric

_device = None


def _read_data():
    """Read current data from device. Return only when the whole data set is ready.
    Returns:
        float, float, float: time [Unix timestamp], co2 [ppm], and temperature [°C]
    """

    # It takes multiple reading from the device to read both co2 and temperature
    co2 = temperature = humid = None
    while co2 is None or temperature is None:

        try:
            data = list(_device.read(8, 10000))  # Times out after 10 s to avoid blocking permanently the thread
        except KeyboardInterrupt:
            _exit()
        except OSError as e:
            print('Could not read the device, check that it is correctly plugged:', e)
            _exit()

        if len(data) == 0:
            print("no data received ...")
            continue

        key = data[0]
        value = data[1] << 8 | data[2]
        if key == 0x50:
            co2 = value
        elif key == 0x42:
            temperature = value / 16.0 - 273.15
        elif key == 0x41:
            humid = value / 100

    return time.time(), co2, temperature, humid


def _exit():
    print('\nExiting ...')
    _device.close()
    sys.exit(0)


def open_device():
    """Prepare the device."""
    global _device
    vendor_id = 0x04d9
    product_id = 0xa052
    # _device = hid.Device(path=sys.argv[1].encode())
    # _device = hid.Device(path="/dev/co2mon".encode())
    # print(f'Device manufacturer: {_device.manufacturer}')
    _device = hid.device()
    _device.open(vendor_id, product_id)


def run(delay=10):
    """Watch the device and call all the callbacks registered with :func:register_watcher once the device returns a data set.
    Parameters:
        delay (int): Data acquisition period in seconds.
    """
    global _device

    if _device is None:
        open_device()

    host = "http://prusapi.v2c2.at:8086"
    query_url = f"{host}/query"
    write_url = f"{host}/write?db=co2mon"
    try:
        req = requests.Request(query_url, data=b"q=CREATE DATABASE co2mon")
        with requests.urlopen(req) as f:
            print(f.read().decode('utf-8'))
    except urllib.error.URLError as e:
        print(e)

    while True:
        t, co2, temperature, humid = _read_data()
        if not t or not co2 or not temperature or not humid:
            continue
        print(f'CO2: {co2:4d}  Temp: {temperature:4.1f}  Humid: {humid:4.1f}')

        m = metric.Metric('mon')
        m.set_values({'CO2': co2, 'Temp': temperature, 'Humid': humid})

        try:
            req = requests.Request(write_url, data=str(m).encode(), method='POST')
            with requests.urlopen(req) as f:
                resp = f.read().decode('utf-8')
                # print(resp)
        except urllib.error.URLError as e:
            print(f'URLError Exception: {e}')

        # Wait until reading further data and handle keyboard interruptions gracefully
        try:
            time.sleep(delay)
        except KeyboardInterrupt:
            _exit()


if __name__ == "__main__":
    run(delay=20)
