import express from "express";
import { processGenericRequest } from "../api/campaignApis";
import { createAndUploadFile, getBoundarySheetData } from "../api/genericApis";
import { getLocalizedName, getResourceDetails, processDataSearchRequest } from "../utils/campaignUtils";
import { addDataToSheet, enrichResourceDetails, getLocalizedMessagesHandler, searchGeneratedResources, searchAllGeneratedResources, processGenerate, throwError, searchCampaignData, searchMappingData, getRelatedDataWithCampaign } from "../utils/genericUtils";
import { getFormattedStringForDebug, logger } from "../utils/logger";
import { validateCreateRequest, validateDownloadRequest, validateSearchRequest } from "../validators/campaignValidators";
import { validateGenerateRequest } from "../validators/genericValidator";
import { getLocaleFromRequestInfo, getLocalisationModuleName } from "../utils/localisationUtils";
import { getBoundaryTabName } from "../utils/boundaryUtils";
import { getNewExcelWorkbook } from "../utils/excelUtils";
import { redis, checkRedisConnection } from "../utils/redisUtils";
import config from '../config/index'
import {callGenerate } from "../utils/generateUtils";
import { attendanceSheetNames, dataRowStatuses, generatedResourceStatuses } from "../config/constants";
import { isCampaignIdOfMicroplan } from "../utils/campaignUtils";
import { generateDataService as generateTemplateDataService } from "./sheetManageService";
import { localityKeyOf } from "../utils/generatedResourceUtils";
import { GenerateTemplateQuery } from "../models/GenerateTemplateQuery";
import { generationtTemplateConfigs } from "../config/generationtTemplateConfigs";


const generateDataService = async (request: express.Request) => {
    await validateGenerateRequest(request);
    logger.info("VALIDATED THE DATA GENERATE REQUEST");
    await processGenerate(request);
    return request?.body?.generatedResource;
};

// boundary stays on the legacy generate flow, which owns the per-level tab splitting the v2 class has no equivalent for.
const legacyOwnedGenerationTypes = new Set<string>(["boundary"]);
const sheetManageGenerationTypes = new Set<string>(
    Object.keys(generationtTemplateConfigs).filter((type) => !legacyOwnedGenerationTypes.has(type))
);

const downloadGeneratedStatusPollIntervalMs = 1000;
const downloadGeneratedStatusPollMaxAttempts = 45;
const downloadResponseTimeBudgetMs = 45 * 1000;
const staleInProgressResourceThresholdMs = 5 * 60 * 1000;
const ALWAYS_FRESH_DOWNLOAD_TYPES = new Set<string>([
    "attendanceRegisterUserBulkMapping"
]);

const DISTRIBUTOR_ROLE_CODE = "DISTRIBUTOR";
const WORKER_ID_COLUMN = "HCM_ADMIN_CONSOLE_USER_WORKER_ID";
const USERNAME_COLUMN = "UserName";
const USER_NAME_COLUMN = "HCM_ADMIN_CONSOLE_USER_NAME";
const ROLE_COLUMN = "HCM_ADMIN_CONSOLE_USER_ROLE";
const BOUNDARY_COLUMN = "HCM_ADMIN_CONSOLE_BOUNDARY_NAME";
const BOUNDARY_CODE_MANDATORY_COLUMN = "HCM_ADMIN_CONSOLE_BOUNDARY_CODE_MANDATORY";
const BOUNDARY_CODE_COLUMN = "HCM_ADMIN_CONSOLE_BOUNDARY_CODE";
const USER_DATA_TYPE = "user";
const USER_SERVICE_UUID_COLUMN = "UserService Uuids";
const MAX_ROLE_COLUMNS = 5;

function toEpoch(value: unknown): number {
    if (typeof value === "number" && Number.isFinite(value)) return value;
    if (typeof value === "string") {
        const parsed = parseInt(value, 10);
        return Number.isFinite(parsed) ? parsed : NaN;
    }
    return NaN;
}

function isStaleInProgressResource(resource: any): boolean {
    if (resource?.status !== generatedResourceStatuses.inprogress) return false;
    const createdTime = toEpoch(resource?.auditDetails?.createdTime);
    if (!Number.isFinite(createdTime)) return false;
    return Date.now() - createdTime > staleInProgressResourceThresholdMs;
}

// Falls back to createdTime so a row without a completion stamp yields a shorter window, never a longer one
function completionEpoch(resource: any): number {
    const completedAt = toEpoch(resource?.auditDetails?.lastModifiedTime);
    if (Number.isFinite(completedAt)) return completedAt;
    return toEpoch(resource?.auditDetails?.createdTime);
}

function isReusableCompletedResource(resource: any): boolean {
    if (resource?.status !== generatedResourceStatuses.completed) return false;
    if (!resource?.fileStoreid) return false;
    const reuseWindowMs = config?.generatedResource?.reuseWindowMs ?? 0;
    if (reuseWindowMs <= 0) return false;
    const completedAt = completionEpoch(resource);
    if (!Number.isFinite(completedAt)) return false;
    return Date.now() - completedAt <= reuseWindowMs;
}

function newestForLocality(resources: any[], localityKey: string | null): any {
    if (!Array.isArray(resources) || resources.length === 0) return undefined;
    let fallback: any = undefined;
    let newest: any = undefined;
    let newestCreatedTime = Number.NEGATIVE_INFINITY;

    for (const resource of resources) {
        if (localityKeyOf(resource) !== localityKey) continue;
        if (!fallback) fallback = resource;
        const createdTime = toEpoch(resource?.auditDetails?.createdTime);
        if (!Number.isFinite(createdTime)) continue;
        if (createdTime > newestCreatedTime) {
            newest = resource;
            newestCreatedTime = createdTime;
        }
    }

    return newest ?? fallback;
}

function sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

function getRemainingTimeMs(deadlineEpochMs: number): number {
    return Math.max(0, deadlineEpochMs - Date.now());
}

function createRequestTimeoutError(operationName: string): any {
    const description = `Request timed out while ${operationName}`;
    const error: any = new Error(description);
    error.status = 504;
    error.code = "REQUEST_TIMEOUT";
    error.description = description;
    return error;
}

async function runWithinRequestBudget<T>(
    operationName: string,
    deadlineEpochMs: number,
    operation: () => Promise<T>
): Promise<T> {
    const remainingTimeMs = getRemainingTimeMs(deadlineEpochMs);
    if (remainingTimeMs <= 0) {
        throw createRequestTimeoutError(operationName);
    }

    let timeoutHandle: NodeJS.Timeout | null = null;
    // This bounds caller wait-time; it does not abort the underlying operation unless the downstream client supports cancellation.
    const timeoutPromise = new Promise<T>((_resolve, reject) => {
        timeoutHandle = setTimeout(() => reject(createRequestTimeoutError(operationName)), remainingTimeMs);
    });

    try {
        return await Promise.race([operation(), timeoutPromise]);
    } finally {
        if (timeoutHandle) clearTimeout(timeoutHandle);
    }
}

async function waitForWithinRequestBudget<T>(
    operationName: string,
    deadlineEpochMs: number,
    promise: Promise<T>
): Promise<T> {
    const remainingTimeMs = getRemainingTimeMs(deadlineEpochMs);
    if (remainingTimeMs <= 0) {
        throw createRequestTimeoutError(operationName);
    }

    let timeoutHandle: NodeJS.Timeout | null = null;
    const timeoutPromise = new Promise<T>((_resolve, reject) => {
        timeoutHandle = setTimeout(() => reject(createRequestTimeoutError(operationName)), remainingTimeMs);
    });

    try {
        return await Promise.race([promise, timeoutPromise]);
    } finally {
        if (timeoutHandle) clearTimeout(timeoutHandle);
    }
}

async function searchGeneratedResourcesWithinDeadline(
    requestQuery: any,
    locale: string,
    deadlineEpochMs: number
): Promise<any[] | null> {
    const remainingTimeMs = getRemainingTimeMs(deadlineEpochMs);
    if (remainingTimeMs <= 0) return null;

    let timeoutHandle: NodeJS.Timeout | null = null;
    const timeoutPromise = new Promise<null>((resolve) => {
        timeoutHandle = setTimeout(() => resolve(null), remainingTimeMs);
    });

    try {
        const result = await Promise.race([
            searchGeneratedResources(requestQuery, locale),
            timeoutPromise
        ]);
        return result as any[] | null;
    } finally {
        if (timeoutHandle) clearTimeout(timeoutHandle);
    }
}

async function waitForGeneratedResourceTerminalStatus(
    requestQuery: any,
    locale: string,
    generatedResourceId: string,
    maxWaitMs: number
): Promise<any | null> {
    if (maxWaitMs <= 0) return null;

    const pollDeadline = Date.now() + maxWaitMs;
    const maxAttempts = Math.min(
        downloadGeneratedStatusPollMaxAttempts,
        Math.max(1, Math.ceil(maxWaitMs / downloadGeneratedStatusPollIntervalMs))
    );

    for (let attempt = 0; attempt < maxAttempts; attempt++) {
        const generatedResources = await searchGeneratedResourcesWithinDeadline(
            { ...requestQuery, id: generatedResourceId },
            locale,
            pollDeadline
        );
        if (generatedResources === null) {
            logger.warn(`Stopping generation polling for id=${generatedResourceId}: polling budget exhausted.`);
            return null;
        }
        const generatedResource = generatedResources?.[0];
        const status = generatedResource?.status;

        if (status === generatedResourceStatuses.completed || status === generatedResourceStatuses.failed) {
            return generatedResource;
        }

        if (attempt < maxAttempts - 1) {
            const remainingMs = pollDeadline - Date.now();
            if (remainingMs <= 0) break;
            await sleep(Math.min(downloadGeneratedStatusPollIntervalMs, remainingMs));
        }
    }
    return null;
}


const downloadDataService = async (request: express.Request) => {
    const requestDeadline = Date.now() + downloadResponseTimeBudgetMs;
    await runWithinRequestBudget(
        "validating download request",
        requestDeadline,
        async () => validateDownloadRequest(request)
    );
    logger.info("VALIDATED THE DATA DOWNLOAD REQUEST");

    const type = String(request.query.type);
    const locale = getLocaleFromRequestInfo(request?.body?.RequestInfo);
    const hasRequestedGeneratedId = Boolean(request?.query?.id);
    const resourceDetailsPromise = runWithinRequestBudget(
        "loading resource details",
        requestDeadline,
        async () => getResourceDetails(request)
    );
    let responseData: any[] | null = [];
    let resourceDetails: any = null;
    const tenantId = String(request?.query?.tenantId || "");
    const hierarchyType = String(request?.query?.hierarchyType || "");
    const campaignId = String(request?.query?.campaignId || "");
    const localityCode = request?.query?.localityCode ? String(request?.query?.localityCode) : undefined;
    const forceUpdate = String(request?.query?.forceUpdate || "") === "true";
    const userUuid = request?.body?.RequestInfo?.userInfo?.uuid || "null";
    const shouldBypassReuse = ALWAYS_FRESH_DOWNLOAD_TYPES.has(type);

    if (!hasRequestedGeneratedId) {
        const requestLocalityKey = localityKeyOf({ additionalDetails: { localityCode } });
        const [resolvedResourceDetails, candidates] = await Promise.all([
            waitForWithinRequestBudget("loading resource details", requestDeadline, resourceDetailsPromise),
            runWithinRequestBudget(
                "searching generated resources",
                requestDeadline,
                async () => searchAllGeneratedResources(
                    {
                        tenantId,
                        type,
                        hierarchyType,
                        campaignId,
                        status: `${generatedResourceStatuses.completed},${generatedResourceStatuses.inprogress}`
                    },
                    locale
                )
            )
        ]);
        resourceDetails = resolvedResourceDetails;
        const latestResource = newestForLocality(candidates || [], requestLocalityKey);
        const hasFreshInProgressResource =
            !shouldBypassReuse
            && latestResource?.status === generatedResourceStatuses.inprogress
            && !isStaleInProgressResource(latestResource);
        const hasReusableCompletedResource =
            !shouldBypassReuse
            && !forceUpdate
            && isReusableCompletedResource(latestResource);

        if (hasFreshInProgressResource || hasReusableCompletedResource) {
            responseData = [latestResource];
            logger.info(
                `Reusing generation id=${latestResource.id} status=${latestResource.status} `
                + `for localityCode=${requestLocalityKey}.`
            );
        } else {
            if (latestResource?.status === generatedResourceStatuses.inprogress) {
                const reason = shouldBypassReuse
                    ? "reuse disabled for this type"
                    : "stale in-progress resource";
                logger.warn(`Found in-progress generation id=${latestResource.id}; creating a fresh one (${reason}).`);
            } else {
                logger.info(`Generating fresh template for download — campaignId=${campaignId}, type=${type}`);
            }

            let isMicroplan = false;
            try {
                isMicroplan = await runWithinRequestBudget(
                    "checking campaign microplan status",
                    requestDeadline,
                    async () => isCampaignIdOfMicroplan(tenantId, campaignId)
                );
            } catch (e: any) {
                if (e?.code === "REQUEST_TIMEOUT") {
                    throw e;
                }
                throwError("COMMON", 500, "INTERNAL_SERVER_ERROR", "Error checking if campaign id is of microplan");
            }

            if (!isMicroplan && sheetManageGenerationTypes.has(type)) {
                const generateTemplateQuery: GenerateTemplateQuery = {
                    type,
                    tenantId,
                    hierarchyType,
                    campaignId,
                    ...(localityCode ? { localityCode } : {}),
                };
                const generatedResource = await runWithinRequestBudget(
                    "generating download template",
                    requestDeadline,
                    async () => generateTemplateDataService(
                        generateTemplateQuery,
                        userUuid,
                        locale,
                        request?.body?.RequestInfo
                    )
                );
                responseData = generatedResource ? [generatedResource] : [];
            } else {
                const newRequestToGenerate = {
                    ...request,
                    query: {
                        ...request.query,
                        type,
                        tenantId,
                        hierarchyType,
                        campaignId,
                        localityCode,
                        forceUpdate: 'true'
                    }
                };
                await runWithinRequestBudget(
                    "triggering legacy template generation",
                    requestDeadline,
                    async () => callGenerate(newRequestToGenerate, type)
                );
                if (Array.isArray(newRequestToGenerate?.body?.generatedResource) && newRequestToGenerate.body.generatedResource.length > 0) {
                    responseData = newRequestToGenerate.body.generatedResource;
                } else {
                    responseData = await runWithinRequestBudget(
                        "searching generated resources",
                        requestDeadline,
                        async () => searchGeneratedResources(newRequestToGenerate?.query, locale)
                    );
                }
            }
        }

        const generatedResourceId = responseData?.[0]?.id;
        if (generatedResourceId && responseData?.[0]?.status === generatedResourceStatuses.inprogress) {
            const remainingBudgetMs = getRemainingTimeMs(requestDeadline);
            const terminalResource = await waitForGeneratedResourceTerminalStatus(
                request?.query,
                locale,
                generatedResourceId,
                remainingBudgetMs
            );
            if (terminalResource) {
                responseData = [terminalResource];
            } else if (remainingBudgetMs <= 0) {
                logger.warn(`Skipping terminal-status wait for generation id=${generatedResourceId}: request budget exhausted.`);
            }
        }
    } else {
        const [generatedResources, resolvedResourceDetails] = await Promise.all([
            runWithinRequestBudget(
                "searching generated resources",
                requestDeadline,
                async () => searchGeneratedResources(request?.query, locale)
            ),
            waitForWithinRequestBudget("loading resource details", requestDeadline, resourceDetailsPromise)
        ]);
        responseData = generatedResources;
        resourceDetails = resolvedResourceDetails;
    }

    if (resourceDetails != null && responseData != null && responseData.length > 0) {
        responseData[0].additionalDetails = {
            ...(responseData[0].additionalDetails || {}),
            ...(resourceDetails?.additionalDetails || {})
        };
    }

    return responseData;
}

const getBoundaryDataService = async (
    request: express.Request, enableCaching = false) => {
    try {
        const { hierarchyType, campaignId } = request?.query;
        const cacheTTL = config?.cacheTime;
        const cacheKey = `${campaignId}-${hierarchyType}`;
        let isRedisConnected = false;
        let cachedData: any = null;
        if (cacheKey && enableCaching) {
            isRedisConnected = await checkRedisConnection();
            cachedData = await redis.get(cacheKey);
        }
        if (cachedData) {
            logger.info("CACHE HIT :: " + cacheKey);
            logger.debug(`CACHED DATA :: ${getFormattedStringForDebug(cachedData)}`);

            if (config.cacheValues.resetCache) {
                await redis.expire(cacheKey, cacheTTL);
            }

            return JSON.parse(cachedData);
        } else {
            logger.info("NO CACHE FOUND :: REQUEST :: " + cacheKey);
        }
        const workbook = getNewExcelWorkbook();
        const localizationMapHierarchy = hierarchyType && await getLocalizedMessagesHandler(request, request?.query?.tenantId, getLocalisationModuleName(hierarchyType), true);
        const localizationMapModule = await getLocalizedMessagesHandler(request, request?.query?.tenantId);
        const localizationMap = { ...(localizationMapHierarchy || {}), ...localizationMapModule };
        const boundarySheetData: any = await getBoundarySheetData(request, localizationMap,enableCaching === true);
        const localizedBoundaryTab = getLocalizedName(getBoundaryTabName(), localizationMap);
        const boundarySheet = workbook.addWorksheet(localizedBoundaryTab);
        addDataToSheet(request, boundarySheet, boundarySheetData, '93C47D', 40, true);
        const boundaryFileDetails: any = await createAndUploadFile(workbook, request);
        logger.info("RETURNS THE BOUNDARY RESPONSE");
        if (cacheKey && isRedisConnected) {
            await redis.set(cacheKey, JSON.stringify(boundaryFileDetails), "EX", cacheTTL);
        }
        return boundaryFileDetails;
    } catch (e: any) {
        console.log(e)
        logger.error(String(e))
        throw (e);
    }
};


const createDataService = async (request: any) => {

    const hierarchyType = request?.body?.ResourceDetails?.hierarchyType;
    const localizationMapHierarchy = hierarchyType && await getLocalizedMessagesHandler(request, request?.body?.ResourceDetails?.tenantId, getLocalisationModuleName(hierarchyType), true);
    const localizationMapModule = await getLocalizedMessagesHandler(request, request?.body?.ResourceDetails?.tenantId);
    const localizationMap = { ...(localizationMapHierarchy || {}), ...localizationMapModule };
    logger.info("Validating data create request")
    await validateCreateRequest(request, localizationMap);
    logger.info("VALIDATED THE DATA CREATE REQUEST");

    await enrichResourceDetails(request);

    await processGenericRequest(request, localizationMap);
    return request?.body?.ResourceDetails;
}

const searchDataService = async (request: any) => {
    await validateSearchRequest(request);
    logger.info("VALIDATED THE DATA GENERATE REQUEST");
    await processDataSearchRequest(request);
    return request?.body?.ResourceDetails;
}

/**
 * Search campaign data service with proper validation
 */
const searchCampaignDataService = async (request: any) => {
    try {
        const searchCriteria = request?.body?.SearchCriteria;
        const pagination = request?.body?.Pagination;

        if (!searchCriteria) {
            throwError("COMMON", 400, "VALIDATION_ERROR", "SearchCriteria is required");
        }

        if (!searchCriteria.tenantId) {
            throwError("COMMON", 400, "VALIDATION_ERROR", "tenantId is required in SearchCriteria");
        }

        if (searchCriteria.uniqueIdentifiers && !Array.isArray(searchCriteria.uniqueIdentifiers)) {
            throwError("COMMON", 400, "VALIDATION_ERROR", "uniqueIdentifiers must be an array");
        }

        const searchParams: any = {
            tenantId: searchCriteria.tenantId,
            type: searchCriteria.type,
            campaignNumber: searchCriteria.campaignNumber,
            status: searchCriteria.status,
            uniqueIdentifiers: searchCriteria.uniqueIdentifiers
        };

        if (pagination) {
            searchParams.offset = pagination.offset || 0;
            searchParams.limit = pagination.limit || 100;

            if (searchParams.limit > 1000) {
                throwError("COMMON", 400, "VALIDATION_ERROR", "limit cannot exceed 1000");
            }
            if (searchParams.offset < 0) {
                throwError("COMMON", 400, "VALIDATION_ERROR", "offset cannot be negative");
            }
        }

        logger.info(`Searching campaign data: ${getFormattedStringForDebug(searchParams)}`);

        const result = await searchCampaignData(searchParams);

        logger.info(`Campaign data search completed: ${result.totalCount} total records found`);
        
        return {
            CampaignData: result.data,
            TotalCount: result.totalCount,
            ...(result.pagination && { Pagination: result.pagination })
        };
        
    } catch (error: any) {
        logger.error("Error in searchCampaignDataService:", error);
        throw error;
    }
}

/**
 * Search mapping data service with proper validation
 */
const searchMappingDataService = async (request: any) => {
    try {
        const searchCriteria = request?.body?.SearchCriteria;
        const pagination = request?.body?.Pagination;

        if (!searchCriteria) {
            throwError("COMMON", 400, "VALIDATION_ERROR", "SearchCriteria is required");
        }

        if (!searchCriteria.tenantId) {
            throwError("COMMON", 400, "VALIDATION_ERROR", "tenantId is required in SearchCriteria");
        }

        const searchParams: any = {
            tenantId: searchCriteria.tenantId,
            type: searchCriteria.type,
            campaignNumber: searchCriteria.campaignNumber,
            status: searchCriteria.status,
            boundaryCode: searchCriteria.boundaryCode,
            uniqueIdentifierForData: searchCriteria.uniqueIdentifierForData
        };

        if (pagination) {
            searchParams.offset = pagination.offset || 0;
            searchParams.limit = pagination.limit || 100;

            if (searchParams.limit > 1000) {
                throwError("COMMON", 400, "VALIDATION_ERROR", "limit cannot exceed 1000");
            }
            if (searchParams.offset < 0) {
                throwError("COMMON", 400, "VALIDATION_ERROR", "offset cannot be negative");
            }
        }

        logger.info(`Searching mapping data: ${getFormattedStringForDebug(searchParams)}`);

        const result = await searchMappingData(searchParams);

        logger.info(`Mapping data search completed: ${result.totalCount} total records found`);
        
        return {
            MappingData: result.data,
            TotalCount: result.totalCount,
            ...(result.pagination && { Pagination: result.pagination })
        };
        
    } catch (error: any) {
        logger.error("Error in searchMappingDataService:", error);
        throw error;
    }
}

function textValue(value: unknown): string {
    return typeof value === "string" ? value.trim() : "";
}

function extractRoleCodes(row: Record<string, unknown>): Set<string> {
    const roles = new Set<string>();
    const roleString = textValue(row[ROLE_COLUMN]);
    if (roleString) {
        for (const role of roleString.split(",")) {
            const normalized = textValue(role).toUpperCase();
            if (normalized) roles.add(normalized);
        }
    }
    for (let i = 1; i <= MAX_ROLE_COLUMNS; i++) {
        const normalized = textValue(row[`HCM_ADMIN_CONSOLE_USER_ROLE_MULTISELECT_${i}`]).toUpperCase();
        if (normalized) roles.add(normalized);
    }
    return roles;
}

interface UserUuidLookup {
    byWorkerId: Map<string, string>;
    byUserName: Map<string, string>;
}

function decryptUserName(encrypted: string, decrypt: (value: string) => string): string {
    try {
        return textValue(decrypt(encrypted));
    } catch {
        return "";
    }
}

/** Attendance sheets may carry the individual/user id instead of the upload worker id, so userName is a second key. */
function buildUserUuidLookup(userRows: unknown, decrypt: (value: string) => string): UserUuidLookup {
    const lookup: UserUuidLookup = { byWorkerId: new Map<string, string>(), byUserName: new Map<string, string>() };
    if (!Array.isArray(userRows)) return lookup;
    for (const userRow of userRows) {
        if (userRow?.isDeleted) continue;
        const uuid = textValue(userRow?.uniqueIdAfterProcess) || textValue(userRow?.data?.[USER_SERVICE_UUID_COLUMN]);
        if (!uuid) continue;
        const workerId = textValue(userRow?.data?.[WORKER_ID_COLUMN]);
        if (workerId) lookup.byWorkerId.set(workerId, uuid);
        const userName = decryptUserName(textValue(userRow?.data?.[USERNAME_COLUMN]), decrypt);
        if (userName && !lookup.byUserName.has(userName)) lookup.byUserName.set(userName, uuid);
    }
    return lookup;
}

const searchDistributorsByDhService = async (request: any) => {
    const searchCriteria = request?.body?.SearchCriteria;
    const pagination = request?.body?.Pagination;

    if (!searchCriteria) {
        throwError("COMMON", 400, "VALIDATION_ERROR", "SearchCriteria is required");
    }

    const tenantId = textValue(searchCriteria.tenantId);
    if (!tenantId) {
        throwError("COMMON", 400, "VALIDATION_ERROR", "tenantId is required in SearchCriteria");
    }

    const requestedCampaignNumber = textValue(searchCriteria.campaignNumber);
    if (!requestedCampaignNumber) {
        throwError("COMMON", 400, "VALIDATION_ERROR", "campaignNumber is required in SearchCriteria");
    }

    if (searchCriteria.localityCodes && !Array.isArray(searchCriteria.localityCodes)) {
        throwError("COMMON", 400, "VALIDATION_ERROR", "localityCodes must be an array");
    }

    // Lazy: a top-level import closes the campaignUtils -> dataManageService -> campaignManageService cycle.
    const { searchProjectTypeCampaignService } = await import("./campaignManageService");
    const { TemplateClass: AttendanceRegisterUserBulkMappingTemplateClass } =
        await import("../generateFlowClasses/attendanceRegisterUserBulkMapping-generateClass");

    const campaignResponse = await searchProjectTypeCampaignService({ tenantId, campaignNumber: requestedCampaignNumber }, request);
    const matchedCampaigns: any[] = Array.isArray(campaignResponse?.CampaignDetails) ? campaignResponse.CampaignDetails : [];
    const campaignDetail = matchedCampaigns.find((campaign) => !campaign?.parentId) ?? matchedCampaigns[0];
    if (!campaignDetail) {
        throwError("CAMPAIGN", 400, "CAMPAIGN_NOT_FOUND", `Campaign not found for campaignNumber ${requestedCampaignNumber}`);
    }
    const campaignId = textValue(campaignDetail?.id);

    const hierarchyType = textValue(searchCriteria.hierarchyType) || textValue(campaignDetail?.hierarchyType);
    if (!hierarchyType) {
        throwError("COMMON", 400, "VALIDATION_ERROR", "hierarchyType is required in SearchCriteria or campaign");
    }

    const rawLocalityCodes = Array.isArray(searchCriteria.localityCodes) ? searchCriteria.localityCodes : [];
    const requestedDhCodes = new Set(
        rawLocalityCodes
            .map((code: unknown) => textValue(code))
            .filter(Boolean)
    );

    const sheetMap = await AttendanceRegisterUserBulkMappingTemplateClass.generate(
        generationtTemplateConfigs.attendanceRegisterUserBulkMapping,
        {
            tenantId,
            campaignId,
            hierarchyType,
            requestInfo: request?.body?.RequestInfo
        },
        {}
    );

    const workerRows = Array.isArray(sheetMap?.[attendanceSheetNames.WORKER]?.data)
        ? sheetMap[attendanceSheetNames.WORKER].data
        : [];

    const userRows = await getRelatedDataWithCampaign(USER_DATA_TYPE, requestedCampaignNumber, tenantId, dataRowStatuses.completed);
    // Lazy: cryptUtils derives its key from config at import time, which must not run for every importer of this service.
    const { decrypt } = await import("../utils/cryptUtils");
    const userUuidLookup = buildUserUuidLookup(userRows, decrypt);

    const distributorsByDh = new Map<string, {
        dhCode: string;
        dhName: string;
        distributors: Array<{ uuid: string; userName: string; name: string }>;
        dedupe: Set<string>;
    }>();

    for (const rawRow of workerRows) {
        const row = (rawRow || {}) as Record<string, unknown>;
        const roleCodes = extractRoleCodes(row);
        if (!roleCodes.has(DISTRIBUTOR_ROLE_CODE)) continue;

        const dhCode = textValue(row[BOUNDARY_CODE_MANDATORY_COLUMN]) || textValue(row[BOUNDARY_CODE_COLUMN]);
        if (!dhCode) continue;
        if (requestedDhCodes.size > 0 && !requestedDhCodes.has(dhCode)) continue;

        const workerId = textValue(row[WORKER_ID_COLUMN]);
        const userName = textValue(row[USERNAME_COLUMN]);
        const name = textValue(row[USER_NAME_COLUMN]);
        const uuid = userUuidLookup.byWorkerId.get(workerId) ?? userUuidLookup.byUserName.get(userName) ?? "";
        const dedupeKey = `${uuid || workerId}::${userName}::${name}`;

        let group = distributorsByDh.get(dhCode);
        if (!group) {
            group = {
                dhCode,
                dhName: textValue(row[BOUNDARY_COLUMN]) || dhCode,
                distributors: [],
                dedupe: new Set<string>()
            };
            distributorsByDh.set(dhCode, group);
        }

        if (group.dedupe.has(dedupeKey)) continue;
        group.dedupe.add(dedupeKey);
        group.distributors.push({ uuid, userName, name });
    }

    const unresolvedUserNames = Array.from(distributorsByDh.values())
        .flatMap((group) => group.distributors)
        .filter((distributor) => !distributor.uuid && distributor.userName)
        .map((distributor) => distributor.userName);
    if (unresolvedUserNames.length > 0) {
        // Lazy: userBatchHandler pulls in the user-creation stack, needed only when the campaign data missed a uuid.
        const { fetchUserUuidsByUserName } = await import("../utils/userBatchHandler");
        const uuidsByUserName = await fetchUserUuidsByUserName(unresolvedUserNames, tenantId, request?.body?.RequestInfo);
        for (const group of distributorsByDh.values()) {
            for (const distributor of group.distributors) {
                if (!distributor.uuid) distributor.uuid = uuidsByUserName.get(distributor.userName) ?? "";
            }
        }
    }

    const sortedGroups = Array.from(distributorsByDh.values())
        .sort((a, b) => a.dhCode.localeCompare(b.dhCode))
        .map((entry) => ({
            dhCode: entry.dhCode,
            dhName: entry.dhName,
            distributors: entry.distributors
        }));

    let offset = 0;
    let limit = sortedGroups.length || 100;
    if (pagination) {
        offset = pagination.offset || 0;
        limit = pagination.limit || 100;
        if (offset < 0) {
            throwError("COMMON", 400, "VALIDATION_ERROR", "offset cannot be negative");
        }
        if (limit < 1 || limit > 1000) {
            throwError("COMMON", 400, "VALIDATION_ERROR", "limit must be between 1 and 1000");
        }
    }

    const pagedGroups = sortedGroups.slice(offset, offset + limit);

    return {
        DHDistributors: pagedGroups,
        TotalCount: sortedGroups.length,
        Pagination: {
            offset,
            limit
        }
    };
}

export {
    generateDataService,
    downloadDataService,
    getBoundaryDataService,
    createDataService,
    searchDataService,
    searchCampaignDataService,
    searchMappingDataService,
    searchDistributorsByDhService
}
