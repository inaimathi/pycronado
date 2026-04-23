# src/pycronado/auth.py
import asyncio
import functools


def requires(*param_names, permissions=None):
    if permissions is None:
        required_perms = None
    elif isinstance(permissions, (list, tuple, set)):
        required_perms = list(permissions)
    else:
        required_perms = [permissions]

    def decorator(method):
        def _check(self, args, kwargs):
            for param_name in param_names:
                value = self.param(param_name)
                if value is None:
                    return (
                        self.jsonerr(f"`{param_name}` parameter is required", 400),
                        kwargs,
                    )
                kwargs[param_name] = value

            if required_perms is not None:
                has_any = any(
                    getattr(self, "has_permission", lambda *_: False)(perm)
                    for perm in required_perms
                )
                if not has_any:
                    return self.jsonerr("forbidden", 403), kwargs

            return None, kwargs

        if asyncio.iscoroutinefunction(method):

            @functools.wraps(method)
            async def async_wrapper(self, *args, **kwargs):
                err, kwargs = _check(self, args, kwargs)
                if err is not None:
                    return err
                return await method(self, *args, **kwargs)

            return async_wrapper
        else:

            @functools.wraps(method)
            def sync_wrapper(self, *args, **kwargs):
                err, kwargs = _check(self, args, kwargs)
                if err is not None:
                    return err
                return method(self, *args, **kwargs)

            return sync_wrapper

    return decorator


class UserMixin:
    def token(self):
        if not hasattr(self, "JWT"):
            self.JWT = self.decoded_jwt()
        return self.JWT

    def issuer(self):
        return self.decoded_jwt()["iss"]

    def user(self):
        jwt = self.decoded_jwt()
        return jwt["user"]

    def username(self):
        return self.user()["username"]

    def user_id(self):
        return f"{self.issuer()}::{self.username()}"

    def permissions(self):
        return self.user().get("permissions", [])

    def has_permission(self, ability, group=None):
        for perm in self.permissions():
            p_group = perm.get("user_group")
            p_ability = perm.get("group_ability")

            if not p_group or not p_ability:
                continue  # malformed row, ignore

            ability_ok = (p_ability == ability) or (p_ability == "*")
            group_ok = group is None or p_group == group or p_group == "*"

            if ability_ok and group_ok:
                return True

        return False
