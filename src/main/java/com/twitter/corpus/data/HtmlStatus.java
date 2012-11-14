package com.twitter.corpus.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import com.google.common.base.Preconditions;

public class HtmlStatus implements Writable {
  // We want to keep track of version to future-proof.
  private static final byte VERSION = 2;

  private byte version;
  private int httpStatusCode;
  private long timestamp;
  private String html;

  public HtmlStatus() {
    this.version = VERSION;
  }

  public HtmlStatus(int httpStatusCode, long timestamp, String html) {
    this.version = VERSION;
    this.httpStatusCode = httpStatusCode;
    this.timestamp = timestamp;
    this.html = Preconditions.checkNotNull(html);
  }

  public int getHttpStatusCode() {
    return httpStatusCode;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public String getHtml() {
    return html;
  }

  /**
   * Deserializes the object.
   */
  public void readFields(DataInput in) throws IOException {
    this.version = in.readByte();
    this.httpStatusCode = in.readInt();
    this.timestamp = in.readLong();
    switch (this.version) {
    case 1:
        this.html = in.readUTF();
        break;
    case VERSION:
        this.html = WritableUtils.readString(in);
        break;

    default:
        throw new IOException("Unknown HtmlStatus version " + this.version);
    }
  }

  /**
   * Serializes this object.
   */
  public void write(DataOutput out) throws IOException {
    out.writeByte(version);
    out.writeInt(httpStatusCode);
    out.writeLong(timestamp);
    if (version == 1) {
        out.writeUTF(html);
    } else {
        WritableUtils.writeString(out, html);
    }
  }

  @Override
  public HtmlStatus clone() {
    return new HtmlStatus(httpStatusCode, timestamp, html);
  }

  @Override
  public String toString() {
    return String.format("[Fetched at %d with status %d:\n%s]\n", timestamp, httpStatusCode, html);
  }
}
