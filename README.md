# Sensor Monitor

```powershell
 $endpoint = New-Object System.Net.IPEndPoint ([System.Net.IPAddress]::Loopback, 3344); $client = New-Object System.Net.Sockets.UdpClient; $bytes = [System.Text.Encoding]::UTF8.GetBytes("sensor_id=t5; value=35.5"); $client.Send($bytes, $bytes.Length, $endpoint); $client.Close()
```

