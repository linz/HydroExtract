<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="2.0">
<xsl:output method="text" encoding="utf-8" />
<xsl:strip-space elements="*" />

<xsl:param name="delim" select="','"/>
<xsl:param name="sep" select="':'"/>
<xsl:param name="quote" select="'&quot;'"/>
<xsl:param name="break" select="'&#xA;'"/>
<xsl:param name="din" select="'{'"/>
<xsl:param name="dout" select="'}'"/>

<xsl:template match="/">
	<xsl:value-of select="$din"/><xsl:apply-templates/><xsl:value-of select="$dout"/>
</xsl:template>

<xsl:template match="*[not(*)]">
    <xsl:value-of select="local-name()"/><xsl:value-of select="$quote"/><xsl:value-of select="$sep"/><xsl:value-of select="concat($quote, normalize-space(), $quote)"/><xsl:value-of select="$delim"/>
</xsl:template>

<xsl:template match="*[(*)]">
	<xsl:if test="not(ancestor::*)">
		<xsl:value-of select="$quote"/>
	</xsl:if>
    <xsl:value-of select="local-name()"/>__<xsl:apply-templates/>
</xsl:template>

</xsl:stylesheet>