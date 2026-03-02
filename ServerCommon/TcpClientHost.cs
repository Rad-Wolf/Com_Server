using System.Net.Sockets;
using System.Text;

namespace ServerCommon;

public sealed class TcpClientHost : IAsyncDisposable
{
	private TcpClient? _client;
	private CancellationTokenSource? _cts;

	public event Action<string>? LogReceived;

	public bool IsConnected => _client?.Connected ?? false;

	public async Task ConnectAsync(string host, int port)
	{
		if (IsConnected)
		{
			Log("이미 연결되어 있습니다.");
			return;
		}

		_cts = new CancellationTokenSource();
		_client = new TcpClient();
		await _client.ConnectAsync(host, port, _cts.Token);
		Log($"서버 연결 성공: {host}:{port}");

		_ = ReceiveLoopAsync(_cts.Token);
	}

	public async Task DisconnectAsync()
	{
		_cts?.Cancel();
		_client?.Close();
		_client = null;
		Log("연결 종료");
		await Task.CompletedTask;
	}

	public async Task SendAsync(string message)
	{
		if (_client is null || !_client.Connected)
		{
			Log("연결 상태가 아니어서 전송할 수 없습니다.");
			return;
		}

		var payload = MessageCodec.Encode(message);
		await _client.GetStream().WriteAsync(payload);
		Log($"[SEND] {message}");
	}

	private async Task ReceiveLoopAsync(CancellationToken token)
	{
		if (_client is null)
		{
			return;
		}

		var stream = _client.GetStream();
		var bytes = new byte[NetworkSettings.BufferSize];
		var receiveBuffer = new StringBuilder();

		try
		{
			while (!token.IsCancellationRequested)
			{
				var read = await stream.ReadAsync(bytes, token);
				if (read == 0)
				{
					break;
				}

				receiveBuffer.Append(Encoding.UTF8.GetString(bytes, 0, read));
				foreach (var message in MessageCodec.DecodeLines(receiveBuffer))
				{
					Log($"[RECV] {message}");
				}
			}
		}
		catch (OperationCanceledException)
		{
		}
		catch (Exception ex)
		{
			Log($"수신 오류: {ex.Message}");
		}
	}

	private void Log(string message) => LogReceived?.Invoke($"[{DateTime.Now:HH:mm:ss}] {message}");

	public async ValueTask DisposeAsync()
	{
		await DisconnectAsync();
		_cts?.Dispose();
	}
}