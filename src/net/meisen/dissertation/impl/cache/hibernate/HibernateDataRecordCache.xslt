<?xml version="1.0" encoding="UTF-8" ?>

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                              xmlns="http://www.springframework.org/schema/beans"
                              xmlns:hibernate="http://dev.meisen.net/xsd/dissertation/caches/records/hibernate">

  <xsl:template match="hibernate:config">
    <bean class="net.meisen.dissertation.impl.cache.hibernate.HibernateDataRecordCacheConfig">
      <xsl:variable name="driver" select="@driver" />
      <property name="driver" value="{$driver}" />
      
      <xsl:variable name="url" select="@url" />
      <property name="url" value="{$url}" />
      
      <xsl:variable name="username" select="@username" />
      <property name="username" value="{$username}" />
      
      <xsl:variable name="password" select="@password" />
      <property name="password" value="{$password}" />

      <xsl:if test="@commitsize">
        <xsl:variable name="commitSize" select="@commitsize" />
        <property name="commitSize" value="{$commitSize}" />
      </xsl:if>
    </bean>
  </xsl:template>
</xsl:stylesheet>