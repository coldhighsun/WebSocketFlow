using System.Net.WebSockets;

namespace WebSocketFlow;

/// <summary>
/// Represents a fully assembled WebSocket message with all fragments merged into a single payload.
/// </summary>
public sealed class WebSocketMessage
{
    /// <summary>
    /// Initializes a new instance of the <see cref="WebSocketMessage"/> class.
    /// </summary>
    /// <param name="messageType">The type of the WebSocket message.</param>
    /// <param name="data">The complete message payload.</param>
    public WebSocketMessage(WebSocketMessageType messageType, ReadOnlyMemory<byte> data)
    {
        MessageType = messageType;
        Data = data;
    }

    /// <summary>
    /// Gets a value indicating whether this message represents a close frame.
    /// </summary>
    public bool CloseReceived => MessageType == WebSocketMessageType.Close;

    /// <summary>
    /// Gets the complete message payload.
    /// </summary>
    public ReadOnlyMemory<byte> Data
    {
        get;
    }

    /// <summary>
    /// Gets the type of the WebSocket message.
    /// </summary>
    public WebSocketMessageType MessageType
    {
        get;
    }
}