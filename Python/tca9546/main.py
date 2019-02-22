#import the I2C library for python
import smbus
from si7020_a20 import SI7020_A20
import tsl2571
import mq131
import vcnl4010
from mcp23008 import mcp23008
from hp203b import hp203b
from sht30 import SHT30
import mcp9600
import adc121c
from sht25 import SHT25
import tca9546


import random
import time
import sys
import iothub_client
from iothub_client import IoTHubClient, IoTHubClientError, IoTHubTransportProvider, IoTHubClientResult
from iothub_client import IoTHubMessage, IoTHubMessageDispositionResult, IoTHubError, DeviceMethodReturnValue
from iothub_client import IoTHubClientRetryPolicy, GetRetryPolicyReturnValue
from iothub_client_args import get_iothub_opt, OptionError

# HTTP options
# Because it can poll "after 9 seconds" polls will happen effectively
# at ~10 seconds.
# Note that for scalabilty, the default value of minimumPollingTime
# is 25 minutes. For more information, see:
# https://azure.microsoft.com/documentation/articles/iot-hub-devguide/#messaging
TIMEOUT = 241000
MINIMUM_POLLING_TIME = 9

# messageTimeout - the maximum time in milliseconds until a message times out.
# The timeout period starts at IoTHubClient.send_event_async.
# By default, messages do not expire.
MESSAGE_TIMEOUT = 10000

RECEIVE_CONTEXT = 0
# AVG_WIND_SPEED = 10.0
MIN_TEMPERATURE = 20.0
MIN_HUMIDITY = 60.0
MESSAGE_COUNT = 1
RECEIVED_COUNT = 0
CONNECTION_STATUS_CONTEXT = 0
TWIN_CONTEXT = 0
SEND_REPORTED_STATE_CONTEXT = 0
METHOD_CONTEXT = 0

# global counters
RECEIVE_CALLBACKS = 0
SEND_CALLBACKS = 0
BLOB_CALLBACKS = 0
CONNECTION_STATUS_CALLBACKS = 0
TWIN_CALLBACKS = 0
SEND_REPORTED_STATE_CALLBACKS = 0
METHOD_CALLBACKS = 0

TIME_BETWEEN_BROADCAST = 5

# chose HTTP, AMQP, AMQP_WS or MQTT as transport protocol
PROTOCOL = IoTHubTransportProvider.MQTT

# String containing Hostname, Device Id & Device Key in the format:
# "HostName=<host_name>;DeviceId=<device_id>;SharedAccessKey=<device_key>"
CONNECTION_STRING_WAREHOUSE = "HostName=Sensor-Data-8266.azure-devices.net;DeviceId=Warehouse_Demo;SharedAccessKey=ZfDsiSdkLB+RJAxsqza3yW0ibEATJflTeTSXpVdX924="
CONNECTION_STRING_ENVIRONMENT = "HostName=Sensor-Data-8266.azure-devices.net;DeviceId=SanFran_Environment;SharedAccessKey=fNjLXfi4jQezNI5XZpqxPhYkuB3rWIe9cNWNvTdOg8Q="
CONNECTION_STRING_INDUSTRIAL = "HostName=Sensor-Data-8266.azure-devices.net;DeviceId=SanFran_Industrial;SharedAccessKey=r5THhT6TEM+sFzZOWaVGUqnUK441ZLAOVdZUqpbE6zY="
CONNECTION_STRING_AIR_COMPRESSOR = "HostName=Sensor-Data-8266.azure-devices.net;DeviceId=SanFran_AirCompressor;SharedAccessKey=MiOtkeQ+/pYnWDv1PdFM3mMf8fu2QlCS2DUXLNBo154="

# MSG_TXT = "{\"Warehouse_Demo\": \"myPythonDevice\",\"windSpeed\": %.2f,\"temperature\": %.2f,\"humidity\": %.2f}"


try:
    # Get I2C bus, I'm using I2C port 1
    bus = smbus.SMBus(1)

    kwargs = {'address': 0x70}
    tca9546 = tca9546.TCA9546(bus, kwargs)

    ##      ENVIRONMENTAL MONITOR DEMO START     ##
    time.sleep(.1)
    tca9546.change_channel(3)
    time.sleep(.1)
    kwargs = {'address': 0x44}
    sht30 = SHT30(bus, kwargs)

    kwargs = {'address': 0x39, 'wait_enable': 0x08, 'als_enable': 0x02, 'power_on': 0x01, 'als_integration_time': 0xFF, 'wait_time': 0xFF, 'gain': 0x20, 'glass_attenuation': 0x01}
    tsl2571 = tsl2571.TSL2571(bus, kwargs)

    kwargs = {'address': 0x50}
    mq131 = mq131.MQ131(bus, kwargs)

    kwargs = {'address': 0x77}
    hp203b = hp203b(bus, kwargs)

    ##      ENVIRONMENTAL MONITOR DEMO END     ##

    ##      INDUSTRIAL EQUIPMENT MONITOR DEMO START     ##
    time.sleep(.1)
    tca9546.change_channel(2)
    time.sleep(.1)

    kwargs = {'address': mcp9600.discover_address(bus), 'sensor_type': 0x00, 'filter': 0x00, 'scale': 0x01, 'sensor_resolution': 0x00, 'adc_resolution': 0x40, 'samples': 0x00, 'mode': 0x00, 'read_register': 0x00}
    print(mcp9600.discover_address(bus))
    mcp9600 = mcp9600.MCP9600(bus, kwargs)

    ##      INDUSTRIAL EQUIPMENT MONITOR DEMO END     ##

    ##      AIR COMPRESSOR DEMO START               ##
    time.sleep(.1)
    tca9546.change_channel(4)
    time.sleep(.1)

    kwargs = {'address': 0x50}
    adc121c = adc121c.ADC121C(bus, kwargs)

    # kwargs = {'address': 0x40}
    # si7020_a20 = SI7020_A20(bus, kwargs)

    kwargs = {'address': 0x40}
    sht25 = SHT25(bus, kwargs)

    ##      AIR COMPRESSOR DEMO END               ##

    ##      WAREHOUSE DEMO START DEF        ##
    time.sleep(.1)
    tca9546.change_channel(1)
    time.sleep(.1)

    gpio_output_map =  {0}
    kwargs = {'address': 0x20, 'gpio_output_map': gpio_output_map}
    mcp23008 = mcp23008(bus, kwargs)

    kwargs = {}

    kwargs = {'address': 0x13}
    vcnl4010 = vcnl4010.VCNL4010(bus, kwargs)

    ##      WAREHOUSE DEMO END DEF      ##
except:
    print('definition fail')

def set_certificates(client):
    from iothub_client_cert import CERTIFICATES
    try:
        client.set_option("TrustedCerts", CERTIFICATES)
        print ( "set_option TrustedCerts successful" )
    except IoTHubClientError as iothub_client_error:
        print ( "set_option TrustedCerts failed (%s)" % iothub_client_error )


def receive_message_callback(message, counter):
    global RECEIVE_CALLBACKS
    message_buffer = message.get_bytearray()
    size = len(message_buffer)
    print ( "Received Message [%d]:" % counter )
    print ( "    Data: <<<%s>>> & Size=%d" % (message_buffer[:size].decode('utf-8'), size) )
    map_properties = message.properties()
    key_value_pair = map_properties.get_internals()
    print ( "    Properties: %s" % key_value_pair )
    counter += 1
    RECEIVE_CALLBACKS += 1
    print ( "    Total calls received: %d" % RECEIVE_CALLBACKS )
    return IoTHubMessageDispositionResult.ACCEPTED


def send_confirmation_callback(message, result, user_context):
    global SEND_CALLBACKS
    print ( "Confirmation[%d] received for message with result = %s" % (user_context, result) )
    map_properties = message.properties()
    print ( "    message_id: %s" % message.message_id )
    print ( "    correlation_id: %s" % message.correlation_id )
    key_value_pair = map_properties.get_internals()
    print ( "    Properties: %s" % key_value_pair )
    SEND_CALLBACKS += 1
    print ( "    Total calls confirmed: %d" % SEND_CALLBACKS )


def connection_status_callback(result, reason, user_context):
    global CONNECTION_STATUS_CALLBACKS
    print ( "Connection status changed[%d] with:" % (user_context) )
    print ( "    reason: %d" % reason )
    print ( "    result: %s" % result )
    CONNECTION_STATUS_CALLBACKS += 1
    print ( "    Total calls confirmed: %d" % CONNECTION_STATUS_CALLBACKS )


def device_twin_callback(update_state, payload, user_context):
    global TWIN_CALLBACKS
    print ( "")
    print ( "Twin callback called with:")
    print ( "updateStatus: %s" % update_state )
    print ( "context: %s" % user_context )
    print ( "payload: %s" % payload )
    TWIN_CALLBACKS += 1
    print ( "Total calls confirmed: %d\n" % TWIN_CALLBACKS )


def send_reported_state_callback(status_code, user_context):
    global SEND_REPORTED_STATE_CALLBACKS
    print ( "Confirmation[%d] for reported state received with:" % (user_context) )
    print ( "    status_code: %d" % status_code )
    SEND_REPORTED_STATE_CALLBACKS += 1
    print ( "    Total calls confirmed: %d" % SEND_REPORTED_STATE_CALLBACKS )


def device_method_callback(method_name, payload, user_context):
    global METHOD_CALLBACKS
    print ( "\nMethod callback called with:\nmethodName = %s\npayload = %s\ncontext = %s" % (method_name, payload, user_context) )
    METHOD_CALLBACKS += 1
    print ( "Total calls confirmed: %d\n" % METHOD_CALLBACKS )
    device_method_return_value = DeviceMethodReturnValue()
    device_method_return_value.response = "{ \"Response\": \"This is the response from the device\" }"
    device_method_return_value.status = 200
    return device_method_return_value


def blob_upload_conf_callback(result, user_context):
    global BLOB_CALLBACKS
    print ( "Blob upload confirmation[%d] received for message with result = %s" % (user_context, result) )
    BLOB_CALLBACKS += 1
    print ( "    Total calls confirmed: %d" % BLOB_CALLBACKS )


def iothub_client_init(uconnection_string):
    # prepare iothub client
    client = IoTHubClient(uconnection_string, PROTOCOL)
    if client.protocol == IoTHubTransportProvider.HTTP:
        client.set_option("timeout", TIMEOUT)
        client.set_option("MinimumPollingTime", MINIMUM_POLLING_TIME)
    # set the time until a message times out
    client.set_option("messageTimeout", MESSAGE_TIMEOUT)
    # some embedded platforms need certificate information
    set_certificates(client)
    # to enable MQTT logging set to 1
    if client.protocol == IoTHubTransportProvider.MQTT:
        client.set_option("logtrace", 0)
    client.set_message_callback(
        receive_message_callback, RECEIVE_CONTEXT)
    if client.protocol == IoTHubTransportProvider.MQTT or client.protocol == IoTHubTransportProvider.MQTT_WS:
        client.set_device_twin_callback(
            device_twin_callback, TWIN_CONTEXT)
        client.set_device_method_callback(
            device_method_callback, METHOD_CONTEXT)
    if client.protocol == IoTHubTransportProvider.AMQP or client.protocol == IoTHubTransportProvider.AMQP_WS:
        client.set_connection_status_callback(
            connection_status_callback, CONNECTION_STATUS_CONTEXT)

    retryPolicy = IoTHubClientRetryPolicy.RETRY_INTERVAL
    retryInterval = 100
    client.set_retry_policy(retryPolicy, retryInterval)
    print ( "SetRetryPolicy to: retryPolicy = %d" %  retryPolicy)
    print ( "SetRetryPolicy to: retryTimeoutLimitInSeconds = %d" %  retryInterval)
    retryPolicyReturn = client.get_retry_policy()
    print ( "GetRetryPolicy returned: retryPolicy = %d" %  retryPolicyReturn.retryPolicy)
    print ( "GetRetryPolicy returned: retryTimeoutLimitInSeconds = %d" %  retryPolicyReturn.retryTimeoutLimitInSeconds)

    return client


def print_last_message_time(client):
    try:
        last_message = client.get_last_message_receive_time()
        print ( "Last Message: %s" % time.asctime(time.localtime(last_message)) )
        print ( "Actual time : %s" % time.asctime() )
    except IoTHubClientError as iothub_client_error:
        if iothub_client_error.args[0].result == IoTHubClientResult.INDEFINITE_TIME:
            print ( "No message received" )
        else:
            print ( iothub_client_error )




def ncd_send_message(msg, client):
    print(msg)
    message = IoTHubMessage(msg)
    message.type = "Telemetry"
    message.message_id = "message_0"
    message.correlation_id = "correlation_0"
    # # optional: assign properties
    prop_map = message.properties()
    client.send_event_async(message, send_confirmation_callback, 0)
    print ( "IoTHubClient.send_event_async accepted message for transmission to IoT Hub.")

def convert_adc_pressure(raw_adc):
    voltage_steps = 0.001220703125
    voltage = raw_adc * voltage_steps
    steps = 4.00 / 174.00
    pressure = (voltage -.48) / steps
    return pressure

def main_loop():

    try:

        client_wh = iothub_client_init(CONNECTION_STRING_WAREHOUSE)
        client_env = iothub_client_init(CONNECTION_STRING_ENVIRONMENT)
        client_ind = iothub_client_init(CONNECTION_STRING_INDUSTRIAL)
        client_ac = iothub_client_init(CONNECTION_STRING_AIR_COMPRESSOR)


        if client_wh.protocol == IoTHubTransportProvider.MQTT:
            print ( "IoTHubClient is reporting state" )
            reported_state = "{\"newState\":\"standBy\",\"featuresDefinitions\":{}}"
            client_wh.send_reported_state(reported_state, len(reported_state), send_reported_state_callback, SEND_REPORTED_STATE_CONTEXT)

        if client_env.protocol == IoTHubTransportProvider.MQTT:
            print ( "IoTHubClient is reporting state" )
            reported_state = "{\"newState\":\"standBy\",\"featuresDefinitions\":{}}"
            client_env.send_reported_state(reported_state, len(reported_state), send_reported_state_callback, SEND_REPORTED_STATE_CONTEXT)

        if client_ind.protocol == IoTHubTransportProvider.MQTT:
            print ( "IoTHubClient is reporting state" )
            reported_state = "{\"newState\":\"standBy\",\"featuresDefinitions\":{}}"
            client_ind.send_reported_state(reported_state, len(reported_state), send_reported_state_callback, SEND_REPORTED_STATE_CONTEXT)

        if client_ac.protocol == IoTHubTransportProvider.MQTT:
            print ( "IoTHubClient is reporting state" )
            reported_state = "{\"newState\":\"standBy\",\"featuresDefinitions\":{}}"
            client_ac.send_reported_state(reported_state, len(reported_state), send_reported_state_callback, SEND_REPORTED_STATE_CONTEXT)

        open_flag = 'Closed'

        open_count = 2500

        while True:
            # send a few messages every minute
            print ( "IoTHubClient sending %d messages" % MESSAGE_COUNT )

            for message_counter in range(0, MESSAGE_COUNT):

                ##      ENVIRONMENTAL DEMO APPLICATION START     ##

                try:
                    time.sleep(.1)
                    tca9546.change_channel(3)
                    time.sleep(.1)
                    sensor_data = sht30.get_readings('fahrenheit')
                    temperature = sensor_data['temperature']
                    humidity = sensor_data['humidity']
                    lux = tsl2571.take_reading()
                    ozone = mq131.take_readings()
                    mbar = hp203b.get_readings()
                    msg_txt_formatted = "{\"deviceId\": \"Environmental Sensor Demo\",\"lux\": "+str(int(lux))+",\"ozone\": "+str(ozone)+",\"temperature\": "+str(int(temperature))+",\"humidity\": "+str(int(humidity))+",\"barometric_pressure\": "+str(int(mbar))+"}"

                    ncd_send_message(msg_txt_formatted, client_env)
                except:
                    print('fail on environmental sensors')
                    sys.exit()

                ##      ENVIRONMENTAL DEMO APPLICATION END     ##

                ##      INDUSTRIAL DEMO APPLICATION START     ##
                try:
                    time.sleep(.1)
                    tca9546.change_channel(2)
                    time.sleep(.1)
                    current = 0
                    ktemperature = mcp9600.take_readings()
                    if(ktemperature < 82):
                        maint_flag = 'False';
                    else:
                        maint_flag = 'True';
                    # ktemperature = 75
                    msg_txt_formatted = "{\"deviceId\": \"Industrial Monitoring Demo\",\"temperature\": "+str(int(ktemperature))+",\"current\": "+str(current)+",\"total_operating_hours\": \"16h 47m\",\"maintenance_required\": \""+maint_flag+"\",\"scheduled_maintenance\": \"5-8-18\"}"

                    ncd_send_message(msg_txt_formatted, client_ind)
                except:
                    print('fail on industrial')
                    sys.exit()

                ##      INDUSTRIAL DEMO APPLICATION END     ##

                ##      AIR COMPRESSOR DEMO START          ##

                try:
                    time.sleep(.1)
                    tca9546.change_channel(4)
                    time.sleep(.1)

                    current = 0
                    voltage = 0
                    wattage = 0
                    pressure = convert_adc_pressure(adc121c.get_single_value())
                    sht25_readings = sht25.get_readings('fahrenheit')
                    temperature = sht25_readings['temperature']
                    humidity = sht25_readings['humidity']
                    barometric_pressure = hp203b.get_readings()

                    msg_txt_formatted = "{\"deviceId\": \"San_Fran_AirCompressor\",\"current\": "+str(current)+",\"voltage\": "+str(voltage)+",\"wattage\": "+str(wattage)+",\"Air Pressure\": "+str(pressure)+",\"barometric_pressure\": "+str(int(barometric_pressure))+",\"temperature\": "+str(int(temperature))+",\"humidity\": "+str(int(humidity))+"}"

                    ncd_send_message(msg_txt_formatted, client_ac)
                except:
                    print('fail on compressor')
                    sys.exit()

                ##      AIR COMPRESSOR DEMO END          ##

                ##      WAREHOUSE DEMO APPLICATION START     ##

                try:
                    time.sleep(.1)
                    tca9546.change_channel(1)
                    time.sleep(.1)

                    proximity_array = vcnl4010.get_readings()
                    if(proximity_array['proximity'] > 4000 and open_flag == 'Closed' or proximity_array['proximity'] <= 4000 and open_flag == 'Open'):
                        mcp23008.toggle_relay(0)
                        time.sleep(.01)
                        mcp23008.toggle_relay(0)
                        if(proximity_array['proximity'] > 4000):
                            open_flag = 'Open'
                            open_count = open_count+1
                        else:
                            open_flag = 'Closed'

                    msg_txt_formatted = "{\"deviceId\": \"Warehouse_Demo\",\"proximity\": "+str(proximity_array['proximity'])+",\"garage_door\": \""+open_flag+"\",\"times_opened\": \""+str(open_count)+"\",\"requires_maintenance\": \"False\"}"

                    ncd_send_message(msg_txt_formatted, client_wh)
                except:
                    print('fail on warehouse')
                    sys.exit()

                ##      WAREHOUSE DEMO APPLICATION END     ##

                # msg_txt_formatted = "{\"deviceId\": \"Warehouse_Demo\"}"
                # msg_txt_formatted = "{\"deviceId\": \"Warehouse_Demo\",\"temperature\": "+str(temperature)+",\"humidity\": "+str(humidity)+",\"lux\": "+str(lux)+",\"ozone\": "+str(ozone)+",\"proximity\": "+str(proximity_array['proximity'])+"}"
                # msg_txt_formatted = "{\"deviceId\": \"Warehouse_Demo\",\"lux\": "+str(lux)+",\"ozone\": "+str(ozone)+",\"proximity\": "+str(proximity_array['proximity'])+",\"adc\": "+str(adc_reading)+",\"current\": "0"}"

                # messages can be encoded as string or bytearray
                # if (message_counter & 1) == 1:
                #     message = IoTHubMessage(bytearray(msg_txt_formatted, 'utf8'))
                # else:
                #     message = IoTHubMessage(msg_txt_formatted)
                # optional: assign ids

                # message.type = "Telemetry"
                # message.message_id = "message_%d" % message_counter
                # message.correlation_id = "correlation_%d" % message_counter

                # optional: assign properties

                # prop_map = message.properties()

                # prop_map.add("temperature", str(temperature))
                # prop_map.add("humidity", str(humidity))
                # prop_map.add("features", "true")


            # Wait for Commands or exit
            print ( "IoTHubClient waiting for commands, press Ctrl-C to exit" )
            start = time.time()
            while(time.time() < start+TIME_BETWEEN_BROADCAST):
                proximity_array = vcnl4010.get_readings()
                if(proximity_array['proximity'] > 4000 and open_flag == 'Closed' or proximity_array['proximity'] <= 4000 and open_flag == 'Open'):
                    mcp23008.toggle_relay(0)
                    time.sleep(.01)
                    mcp23008.toggle_relay(0)
                    if(proximity_array['proximity'] > 4000):
                        open_flag = 'Open'
                        open_count = open_count+1
                    else:
                        open_flag = 'Closed'

            # time.sleep(TIME_BETWEEN_BROADCAST)

            # status_counter = 0
            # while status_counter <= MESSAGE_COUNT:
            #     status = client.get_send_status()
            #     print ( "Send status: %s" % status )
            #     time.sleep(1)
            #     status_counter += 1

    except IoTHubError as iothub_error:
        print ( "Unexpected error %s from IoTHub" % iothub_error )
        return
    except KeyboardInterrupt:
        print ( "IoTHubClient sample stopped" )

    # print_last_message_time(client)


def usage():
    print ( "Usage: iothub_client_sample.py -p <protocol> -c <connectionstring>" )
    print ( "    protocol        : <amqp, amqp_ws, http, mqtt, mqtt_ws>" )
    print ( "    connectionstring: <HostName=<host_name>;DeviceId=<device_id>;SharedAccessKey=<device_key>>" )





print ( "\nPython %s" % sys.version )
print ( "IoT Hub Client for Python" )

try:
    (CONNECTION_STRING_WAREHOUSE, PROTOCOL) = get_iothub_opt(sys.argv[1:], CONNECTION_STRING_WAREHOUSE, PROTOCOL)
    (CONNECTION_STRING_ENVIRONMENT, PROTOCOL) = get_iothub_opt(sys.argv[1:], CONNECTION_STRING_ENVIRONMENT, PROTOCOL)
    (CONNECTION_STRING_INDUSTRIAL, PROTOCOL) = get_iothub_opt(sys.argv[1:], CONNECTION_STRING_INDUSTRIAL, PROTOCOL)
    (CONNECTION_STRING_AIR_COMPRESSOR, PROTOCOL) = get_iothub_opt(sys.argv[1:], CONNECTION_STRING_AIR_COMPRESSOR, PROTOCOL)
except OptionError as option_error:
    print ( option_error )
    usage()
    sys.exit(1)

print ( "Starting the IoT Hub Python sample..." )
print ( "    Protocol %s" % PROTOCOL )
# print ( "    Connection string=%s" % CONNECTION_STRING )

main_loop()
