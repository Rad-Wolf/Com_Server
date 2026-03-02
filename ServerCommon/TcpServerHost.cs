using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace ServerCommon;

public sealed class TcpServerHost : IAsyncDisposable
{
	private readonly ConcurrentDictionary<int, TcpClient> _clients = new();
	private readonly SemaphoreSlim _clientSendLock = new(1, 1);
	private TcpListener? _listener;
	private CancellationTokenSource? _cts;
	private int _clientSequence;

	public event Action<string>? LogReceived;
	public event Action<int, string>? MessageReceived;

	public bool IsRunning => _listener is not null;

	public Task StartAsync(int port)
	{
		if (port is < 1 or > 65535)
		{
			throw new ArgumentOutOfRangeException(nameof(port), "port는 1~65535 사이여야 합니다.");
		}

		if (IsRunning)
		{
			Log($"서버가 이미 실행 중입니다. Port={port}");
			return Task.CompletedTask;
		}

		_cts = new CancellationTokenSource();
		_listener = new TcpListener(IPAddress.Any, port);
		_listener.Start();
		Log($"서버 시작: 0.0.0.0:{port}");

		_ = AcceptLoopAsync(_cts.Token);
		return Task.CompletedTask;
	}

	public async Task StopAsync()
	{
		if (_listener is null)
		{
			return;
		}

		_cts?.Cancel();
		_listener.Stop();
		_listener = null;

		foreach (var (_, client) in _clients)
		{
			client.Close();
			client.Dispose();
		}

		_clients.Clear();

		_cts?.Dispose();
		_cts = null;

		Log("서버가 중지되었습니다.");
		await Task.CompletedTask;
	}

	private async Task AcceptLoopAsync(CancellationToken token)
	{
		while (!token.IsCancellationRequested)
		{
			try
			{
				var listener = _listener;
				if (listener is null)
				{
					break;
				}

				var client = await listener.AcceptTcpClientAsync(token);
				var clientId = Interlocked.Increment(ref _clientSequence);
				_clients.TryAdd(clientId, client);

				Log($"클라이언트 연결됨: #{clientId} ({client.Client.RemoteEndPoint})");
				_ = HandleClientAsync(clientId, client, token);
			}
			catch (OperationCanceledException)
			{
				break;
			}
			catch (Exception ex)
			{
				Log($"AcceptLoop 오류: {ex.Message}");
			}
		}
	}

	private async Task HandleClientAsync(int clientId, TcpClient client, CancellationToken token)
	{
		try
		{
			using var stream = client.GetStream();
			var bytes = new byte[NetworkSettings.BufferSize];
			var receiveBuffer = new StringBuilder();

			while (!token.IsCancellationRequested && client.Connected)
			{
				var read = await stream.ReadAsync(bytes, token);
				if (read == 0)
				{
					break;
				}

				receiveBuffer.Append(Encoding.UTF8.GetString(bytes, 0, read));

				foreach (var message in MessageCodec.DecodeLines(receiveBuffer))
				{
					Log($"[RECV #{clientId}] {message}");
					MessageReceived?.Invoke(clientId, message);

					var echo = MessageCodec.Encode($"ACK:{message}");
					await _clientSendLock.WaitAsync(token);
					try
					{
						await stream.WriteAsync(echo, token);
					}
					finally
					{
						_clientSendLock.Release();
					}
				}
			}
		}
		catch (OperationCanceledException)
		{
		}
		catch (Exception ex)
		{
			Log($"클라이언트 처리 오류 #{clientId}: {ex.Message}");
		}
		finally
		{
			_clients.TryRemove(clientId, out _);
			client.Close();
			client.Dispose();
			Log($"클라이언트 종료됨: #{clientId}");
		}
	}

	private void Log(string message) => LogReceived?.Invoke($"[{DateTime.Now:HH:mm:ss}] {message}");

	public async ValueTask DisposeAsync()
	{
		await StopAsync();
		_clientSendLock.Dispose();
	}
}