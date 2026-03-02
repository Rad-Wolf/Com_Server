using System.Text;

namespace ServerCommon;

public static class MessageCodec
{
	public static byte[] Encode(string message)
	{
		var normalized = message.Replace("\r", string.Empty).Replace("\n", string.Empty);
		return Encoding.UTF8.GetBytes($"{normalized}\n");
	}

	public static IEnumerable<string> DecodeLines(StringBuilder receiveBuffer)
	{
		var messages = new List<string>();

		while (true)
		{
			var snapshot = receiveBuffer.ToString();
			var idx = snapshot.IndexOf('\n');
			if (idx < 0)
			{
				break;
			}

			var line = snapshot[..idx].TrimEnd('\r');
			messages.Add(line);
			receiveBuffer.Remove(0, idx + 1);
		}

		return messages;
	}
}