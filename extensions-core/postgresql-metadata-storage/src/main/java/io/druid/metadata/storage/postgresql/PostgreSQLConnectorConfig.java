package io.druid.metadata.storage.postgresql;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.metadata.PasswordProvider;

public class PostgreSQLConnectorConfig
{
    @JsonProperty
    private boolean useSSL = false;

    @JsonProperty("sslPassword")
    private PasswordProvider sslPasswordProvider;

    @JsonProperty
    private String sslFactory;

    @JsonProperty
    private String sslFactoryArg;

    @JsonProperty
    private String sslMode;

    @JsonProperty
    private String sslCert;

    @JsonProperty
    private String sslKey;

    @JsonProperty
    private String sslRootCert;

    @JsonProperty
    private String sslHostNameVerifier;

    @JsonProperty
    private String sslPasswordCallback;


    public boolean isUseSSL()
    {
        return useSSL;
    }

    public String getPassword()
    {
      return sslPasswordProvider == null ? null : sslPasswordProvider.getPassword();
    }

    public String getSslFactory()
    {
      return sslFactory;
    }

    public String getSslFactoryArg()
    {
      return sslFactoryArg;
    }

    public String getSslMode()
    {
      return sslMode;
    }

    public String getSslCert()
    {
      return sslCert;
    }

    public String getSslKey()
    {
      return sslKey;
    }

    public String getSslRootCert()
    {
      return sslRootCert;
    }

    public String getSslHostNameVerifier()
    {
      return sslHostNameVerifier;
    }

    public String getSslPasswordCallback()
    {
      return sslPasswordCallback;
    }

    @Override
    public String toString()
    {
      return "PostgreSQLConnectorConfig{" +
                "useSSL='" + useSSL + '\'' +
                ", sslFactory='" + sslFactory + '\'' +
                ", sslFactoryArg='" + sslFactoryArg + '\'' +
                ", sslMode='" + sslMode + '\'' +
                ", sslCert='" + sslCert + '\'' +
                ", sslKey='" + sslKey + '\'' +
                ", sslRootCert='" + sslRootCert + '\'' +
                ", sslHostNameVerifier='" + sslHostNameVerifier + '\'' +
                ", sslPasswordCallback='" + sslPasswordCallback + '\'' +
                '}';
    }
}
