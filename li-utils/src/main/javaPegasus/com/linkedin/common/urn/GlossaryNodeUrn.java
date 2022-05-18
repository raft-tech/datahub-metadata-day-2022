package com.linkedin.common.urn;

import com.linkedin.data.template.Custom;
import com.linkedin.data.template.DirectCoercer;
import com.linkedin.data.template.TemplateOutputCastException;

import java.net.URISyntaxException;


public final class GlossaryNodeUrn extends Urn {

    public static final String ENTITY_TYPE = "glossaryNode";

    private final String _name;

    public GlossaryNodeUrn(String name) {
        super(ENTITY_TYPE, TupleKey.create(name));
        this._name = name;
    }

    public String getNameEntity() {
        return _name;
    }

    public static GlossaryNodeUrn createFromString(String rawUrn) throws URISyntaxException {
        return createFromUrn(Urn.createFromString(rawUrn));
    }

    public static GlossaryNodeUrn createFromUrn(Urn urn) throws URISyntaxException {
        if (!"li".equals(urn.getNamespace())) {
            throw new URISyntaxException(urn.toString(), "Urn namespace type should be 'li'.");
        } else if (!ENTITY_TYPE.equals(urn.getEntityType())) {
            throw new URISyntaxException(urn.toString(), "Urn entity type should be 'glossaryNode'.");
        } else {
            TupleKey key = urn.getEntityKey();
            if (key.size() != 1) {
                throw new URISyntaxException(urn.toString(), "Invalid number of keys.");
            } else {
                try {
                    return new GlossaryNodeUrn((String) key.getAs(0, String.class));
                } catch (Exception var3) {
                    throw new URISyntaxException(urn.toString(), "Invalid URN Parameter: '" + var3.getMessage());
                }
            }
        }
    }

    public static GlossaryNodeUrn deserialize(String rawUrn) throws URISyntaxException {
        return createFromString(rawUrn);
    }

    static {
        Custom.registerCoercer(new DirectCoercer<GlossaryNodeUrn>() {
            public Object coerceInput(GlossaryNodeUrn object) throws ClassCastException {
                return object.toString();
            }

            public GlossaryNodeUrn coerceOutput(Object object) throws TemplateOutputCastException {
                try {
                    return GlossaryNodeUrn.createFromString((String) object);
                } catch (URISyntaxException e) {
                    throw new TemplateOutputCastException("Invalid URN syntax: " + e.getMessage(), e);
                }
            }
        }, GlossaryNodeUrn.class);
    }

}
