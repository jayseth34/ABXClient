using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Linq;

class Program
{
	const string serverAddress = "127.0.0.1"; // Change if needed
	const int port = 3000;
	const string logFile = "logs.txt";

	static Dictionary<int, Packet> receivedPackets = new();

	static void Main()
	{
		Log("Starting ABX Client...");

		try
		{
			// Step 1: Stream all packets
			StreamAllPackets();

			// Step 2: Detect missing sequences
			List<int> missingSeqs = FindMissingSequences();

			Log($"Missing {missingSeqs.Count} packets: {string.Join(", ", missingSeqs)}");

			// Step 3: Resend missing packets
			foreach (var seq in missingSeqs)
			{
				ResendPacket(seq);
			}

			// Step 4: Display all valid packets
			foreach (var packet in receivedPackets.Values.OrderBy(p => p.Sequence))
			{
				Console.WriteLine(packet);
			}

			Log("Successfully retrieved and validated all packets.");
		}
		catch (Exception ex)
		{
			Log($"Fatal error: {ex.Message}");
		}
	}

	static void StreamAllPackets()
	{
		try
		{
			using TcpClient client = new TcpClient(serverAddress, port);
			using NetworkStream stream = client.GetStream();

			stream.Write(new byte[] { 1, 0 }); // CallType 1

			byte[] buffer = new byte[17];
			while (true)
			{
				int totalRead = ReadExact(stream, buffer, 17);
				if (totalRead == 0)
					break;

				if (totalRead < 17)
				{
					Log($"Partial packet received with only {totalRead} bytes. Skipping.");
					break;
				}

				if (TryParsePacket(buffer, out Packet packet))
				{
					receivedPackets[packet.Sequence] = packet;
				}
				else
				{
					Log("Invalid packet skipped during streaming.");
				}
			}

			Log($"Stream complete. Total valid packets received: {receivedPackets.Count}");
		}
		catch (Exception ex)
		{
			Log($"Error during StreamAllPackets: {ex.Message}");
		}
	}

	static void ResendPacket(int sequence)
	{
		try
		{
			using TcpClient client = new TcpClient(serverAddress, port);
			using NetworkStream stream = client.GetStream();

			stream.Write(new byte[] { 2, (byte)sequence }); // CallType 2

			byte[] buffer = new byte[17];
			int totalRead = ReadExact(stream, buffer, 17);

			if (totalRead == 17)
			{
				if (TryParsePacket(buffer, out Packet packet))
				{
					receivedPackets[packet.Sequence] = packet;
					Log($"Recovered missing packet #{sequence}");
				}
				else
				{
					Log($"Invalid packet received for sequence #{sequence}");
				}
			}
			else
			{
				Log($"Failed to receive packet #{sequence}. Only read {totalRead} bytes.");
			}
		}
		catch (Exception ex)
		{
			Log($"Error during ResendPacket({sequence}): {ex.Message}");
		}
	}

	static int ReadExact(NetworkStream stream, byte[] buffer, int length)
	{
		int totalRead = 0;
		while (totalRead < length)
		{
			int bytesRead = stream.Read(buffer, totalRead, length - totalRead);
			if (bytesRead == 0)
			{
				// End of stream
				return totalRead;
			}
			totalRead += bytesRead;
		}
		return totalRead;
	}

	static List<int> FindMissingSequences()
	{
		List<int> missing = new();
		if (receivedPackets.Count == 0) return missing;

		int maxSeq = receivedPackets.Keys.Max();

		for (int i = 1; i <= maxSeq; i++)
		{
			if (!receivedPackets.ContainsKey(i))
				missing.Add(i);
		}

		return missing;
	}

	static bool TryParsePacket(byte[] data, out Packet packet)
	{
		packet = null!;
		try
		{
			string symbol = Encoding.ASCII.GetString(data, 0, 4);
			char side = (char)data[4];
			int quantity = BitConverter.ToInt32(data[5..9].Reverse().ToArray(), 0);
			int price = BitConverter.ToInt32(data[9..13].Reverse().ToArray(), 0);
			int sequence = BitConverter.ToInt32(data[13..17].Reverse().ToArray(), 0);

			// Validate
			if (!symbol.All(c => c >= 32 && c <= 126)) return false;
			if (side != 'B' && side != 'S') return false;
			if (quantity < 0 || price < 0 || sequence <= 0) return false;

			packet = new Packet(symbol.Trim(), side, quantity, price, sequence);
			return true;
		}
		catch
		{
			return false;
		}
	}

	static void Log(string message)
	{
		string timestamped = $"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] {message}";
		Console.WriteLine(timestamped);
		File.AppendAllText(logFile, timestamped + Environment.NewLine);
	}

	record Packet(string Symbol, char Side, int Quantity, int Price, int Sequence)
	{
		public override string ToString()
			=> $"[{Sequence}] {Symbol} {Side} Qty:{Quantity} Price:{Price}";
	}
}
